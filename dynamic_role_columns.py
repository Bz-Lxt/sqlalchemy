"""
部署相关的动态列（priv_docs / priv_payments / ...）支持。

场景
----
``roles`` 表在不同部署里可能多出 ``priv_docs``、``priv_payments`` 等列，
而模型里只声明 ``id``、``name``。希望：

* ``select(Role)`` 能把这些实际存在的列读到对象上；
* 对这些列的赋值能随正常的 ORM flush（INSERT/UPDATE）落库；
* 不把每个动态列写进映射定义；
* 同一个模型类可以配合不同的 ``schema_translate_map`` 会话使用。

原理
----
1. SQLAlchemy 的 mapper 在 *配置阶段* 会自动把 mapped Table 上所有未显式
   声明的列也映射成属性（属性名 = 列名），因此在 mapper 配置前往
   ``Role.__table__`` 上 :meth:`Table.append_column` 即可；
2. 若 mapper 已经配置过，``append_column`` 之后再调用
   :meth:`Mapper.add_property`，属性会立即装配生效（flush/select 都会带上）；
3. 反射所需的真实 schema 名从会话/语句的 ``schema_translate_map``
   执行选项中解析，所以表定义保持中立，translate map 照常工作。

进程内只反射一次：同一进程内通过 translate map 访问的所有 schema 应当具有
一致的列集合（同一部署的多租户 schema 天然满足；不同部署 = 不同进程）。

异步引擎（AsyncEngine/AsyncSession）场景下，把 :func:`reflect_dynamic_columns`
放在 ``await engine.run_sync(...)`` 里调用即可，事件钩子同样会在底层同步
Session 上触发。
"""

from __future__ import annotations

import threading
from typing import Any, List, Mapping, Optional

from sqlalchemy import Column, event, inspect
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
)


class Base(DeclarativeBase):
    pass


class Role(Base):
    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    # 部署私有的额外列（priv_docs / priv_payments / ...）故意不在此声明：
    # 它们在运行时从真实表反射得到（见 reflect_dynamic_columns）。


_lock = threading.Lock()
_discovered: bool = False
_installed: bool = False


def _resolve_schema(execution_options: Mapping[str, Any]) -> Optional[str]:
    """把 Table 上（可能是占位的）schema 名按 schema_translate_map 解析。

    Table 没有显式 schema 时，translate map 用 ``None`` 作为键。
    """
    translate_map = execution_options.get("schema_translate_map") or {}
    table_schema = Role.__table__.schema
    return translate_map.get(table_schema, table_schema)


def reflect_dynamic_columns(bind: Any, *, schema: Optional[str] = None) -> List[str]:
    """反射 ``bind`` 上 ``roles`` 表的实际列，把模型未声明的列追加并映射。

    :param bind: Engine 或 Connection（用于反射）。
    :param schema: 真实 schema 名。通常不需要手传——事件钩子会按会话的
        ``schema_translate_map`` 自动解析；显式调用（如启动时预热）时，
        传入 translate 之后的真实 schema 名。
    :returns: 本次新增的列名列表。

    幂等；线程安全。
    """
    global _discovered
    added: List[str] = []
    with _lock:
        table = Role.__table__
        mapper = Role.__mapper__
        configured = mapper.configured

        present = set(table.c.keys())
        for col_info in inspect(bind).get_columns(table.name, schema=schema):
            name = col_info["name"]
            if name in present:
                continue
            column = Column(
                name,
                col_info["type"],
                nullable=col_info.get("nullable", True),
            )
            table.append_column(column)
            present.add(name)
            added.append(name)
            if configured:
                # mapper 已配置：追加表列后立即补上映射属性。
                mapper.add_property(name, column)

        _discovered = True
    if added:
        print(f"[dynamic_columns] discovered and mapped: {added}")
    return added


def install_dynamic_columns() -> None:
    """注册会话事件钩子，在首次 ORM 使用时惰性反射动态列。幂等。"""
    global _installed
    if _installed:
        return
    _installed = True

    @event.listens_for(Session, "do_orm_execute")
    def _reflect_before_execute(state: Any) -> None:
        # 在语句编译/执行前触发：此时追加的列会进入本次 SELECT/INSERT/
        # UPDATE/DELETE 的列集合。
        if _discovered or Role.__mapper__ not in state.all_mappers:
            return
        schema = _resolve_schema(state.execution_options)
        reflect_dynamic_columns(
            state.session.get_bind(Role.__mapper__), schema=schema
        )

    @event.listens_for(Session, "before_flush")
    def _reflect_before_flush(session: Session, ctx: Any, instances: Any) -> None:
        # 兜底：进程内第一次 ORM 操作就是 flush（没有先 select）的场景。
        if _discovered:
            return
        involved = set(session.new) | set(session.dirty) | set(session.deleted)
        if not any(isinstance(obj, Role) for obj in involved):
            return
        schema = _resolve_schema(session.execution_options)
        reflect_dynamic_columns(session.get_bind(Role.__mapper__), schema=schema)


install_dynamic_columns()


# ---------------------------------------------------------------------------
# 演示 / 验证：用 SQLite 的 ATTACH DATABASE 模拟同一引擎下的多个部署 schema。
#
#   python dynamic_role_columns.py base        # 裸部署：只有 id/name
#   python dynamic_role_columns.py full        # 带 priv_docs/priv_payments 的部署
#   python dynamic_role_columns.py flushfirst  # 首次操作就是 flush 的场景
# ---------------------------------------------------------------------------
def _make_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    # 单连接内存库，保证 ATTACH 的附加库在整个演示过程中存活。
    return create_engine("sqlite://", poolclass=StaticPool)


def _attach_deployments(engine, *names):
    from sqlalchemy import text

    with engine.begin() as conn:
        for name in names:
            conn.execute(text(f"ATTACH DATABASE ':memory:' AS {name}"))


def _scenario_base():
    """部署 A：roles 表只有 id、name。"""
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    engine = _make_engine()
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        s.add(Role(name="admin"))
        s.commit()

        role = s.scalars(select(Role)).one()
        assert role.id == 1 and role.name == "admin"
        assert "priv_docs" not in Role.__table__.c
        print("[base] OK: 只有 id/name，无动态列，CRUD 正常")


def _scenario_full():
    """部署 B：roles 表含 priv_docs、priv_payments；多个 translate-map 会话。"""
    from sqlalchemy import select, text
    from sqlalchemy.orm import Session

    engine = _make_engine()
    _attach_deployments(engine, "deploy_a", "deploy_b")
    with engine.begin() as conn:
        for sch, seed in (
            ("deploy_a", {"name": "admin", "d": 1, "p": 0}),
            ("deploy_b", {"name": "billing", "d": 0, "p": 1}),
        ):
            conn.execute(
                text(
                    f"CREATE TABLE {sch}.roles ("
                    "id INTEGER PRIMARY KEY, name TEXT, "
                    "priv_docs BOOLEAN, priv_payments BOOLEAN)"
                )
            )
            conn.execute(
                text(
                    f"INSERT INTO {sch}.roles "
                    "(name, priv_docs, priv_payments) VALUES (:name, :d, :p)"
                ),
                seed,
            )

    # 会话 1：translate map 指向 deploy_a —— 首次 ORM 操作触发反射。
    with Session(
        engine, execution_options={"schema_translate_map": {None: "deploy_a"}}
    ) as s:
        role = s.scalars(select(Role)).one()
        assert role.name == "admin"
        # 1) 读取：动态列已加载到对象上
        assert role.priv_docs is True and role.priv_payments is False, (
            role.priv_docs,
            role.priv_payments,
        )

        # 2) 更新动态列 → flush 必须落库
        role.priv_docs = False
        s.flush()
        raw = s.execute(
            text("SELECT priv_docs FROM deploy_a.roles WHERE id = :i"),
            {"i": role.id},
        ).scalar_one()
        assert raw == 0, raw

        # 3) 插入新对象时动态列随 INSERT 落库（反射后属性已映射，可走构造器）
        s.add(Role(name="editor", priv_docs=False, priv_payments=True))
        s.commit()
        raw = s.execute(
            text(
                "SELECT priv_docs, priv_payments FROM deploy_a.roles "
                "WHERE name = 'editor'"
            )
        ).one()
        assert raw == (0, 1), raw
        print("[full] deploy_a: 读取/UPDATE/INSERT 动态列均生效")

    # 会话 2：同一个模型类，translate map 指向另一个 schema（同构）。
    with Session(
        engine, execution_options={"schema_translate_map": {None: "deploy_b"}}
    ) as s:
        role = s.scalars(select(Role)).one()
        assert role.name == "billing"
        assert role.priv_docs is False and role.priv_payments is True
        print("[full] deploy_b: 同一模型 + 不同 schema_translate_map 会话正常")


def _scenario_flushfirst():
    """首次 ORM 操作就是 flush（反射在 before_flush 中完成）。"""
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    engine = _make_engine()
    _attach_deployments(engine, "deploy_a")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE deploy_a.roles ("
                "id INTEGER PRIMARY KEY, name TEXT, "
                "priv_docs BOOLEAN, priv_payments BOOLEAN)"
            )
        )

    with Session(
        engine, execution_options={"schema_translate_map": {None: "deploy_a"}}
    ) as s:
        role = Role(name="first")
        # 此时映射里还没有动态列，按普通属性赋值；
        # before_flush 反射补映射后，值随 INSERT 落库。
        role.priv_docs = True
        role.priv_payments = False
        s.add(role)
        s.commit()

        raw = s.execute(
            text(
                "SELECT name, priv_docs, priv_payments FROM deploy_a.roles "
                "WHERE name = 'first'"
            )
        ).one()
        assert raw == ("first", 1, 0), raw
        print("[flushfirst] 首次操作即 flush：动态列随 INSERT 落库")


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    {"base": _scenario_base, "full": _scenario_full, "flushfirst": _scenario_flushfirst}[
        mode
    ]()
