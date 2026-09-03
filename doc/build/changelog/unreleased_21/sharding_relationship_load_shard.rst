.. change::
    :tags: bug, orm extensions
    :versions: 2.1.0

    Fixed issue in the ``sqlalchemy.ext.horizontal_shard`` extension where
    relationship loads that occur on behalf of objects already loaded from a
    specific shard would scan **all** shards.  Eager loaders such as
    :func:`_orm.selectinload` now limit their secondary query to the shard that
    the parent rows were loaded from, and lazy relationship loaders (as well
    as :func:`_orm.lazyload`) now use the shard identity token carried by the
    parent instance instead of invoking the execute chooser across every
    shard. This means relationship loading no longer requires an explicit
    :class:`_horizontal.set_shard_id` option in order to avoid querying
    shards that cannot contain the related rows.  The
    :class:`_horizontal.set_shard_id` option continues to apply an explicit
    shard; when constructed with ``propagate_to_loaders=False``, lazy
    relationship loaders continue to defer to the execute chooser as before,
    while eager and column loaders still follow the shard of the parent row.
