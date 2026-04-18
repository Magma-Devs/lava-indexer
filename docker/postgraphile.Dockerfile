# Extends the stock postgraphile image with two plugins the Magma-Devs/info
# API depends on:
#   - postgraphile-plugin-connection-filter: adds `filter: { x: { op: y } }`
#     with range operators (greaterThanOrEqualTo, lessThanOrEqualTo, in, …).
#     Vanilla postgraphile only exposes `condition: { x: y }` (exact match),
#     which is insufficient for the API's date-range queries.
#   - @graphile/pg-aggregates: adds `aggregates { sum { … } }` and
#     `groupedAggregates(groupBy: …)` on every connection. Required for
#     /stats, /top-chains, and the per-provider / per-spec rollups.
#
# Both are referenced via --append-plugins in docker-compose.yml.
FROM graphile/postgraphile:4
# postgraphile's CLI does require('<plugin>') from /postgraphile/build/postgraphile/,
# so Node resolves modules via the standard walk-up path. A global install
# (`npm i -g`) lands in a prefix that isn't on that path and the require
# fails with MODULE_NOT_FOUND. Installing into /postgraphile/node_modules
# puts the plugins exactly where the walk-up looks first.
WORKDIR /postgraphile
# --legacy-peer-deps: postgraphile v4 pins graphile-build versions that
# differ slightly from what these plugins peerDepend on. The plugins work
# fine at runtime with v4's bundled graphile-build, but npm's strict peer
# resolver refuses the install without this flag.
#
# Pin the plugin majors: postgraphile v4's `--append-plugins` CLI calls
# `require(name)` and expects the result to be a function. v2 of
# connection-filter and v0.1.x of pg-aggregates default-export that
# function; later majors switched to `{ default: fn }`, which the v4 CLI
# can't unwrap.
RUN npm install --legacy-peer-deps --omit=dev \
      postgraphile-plugin-connection-filter@^2 \
      @graphile/pg-aggregates@^0.1
