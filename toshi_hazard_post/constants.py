"""The prefix of shared memory segment names. Each run makes its own names, in aggregation.run_aggregation."""

# Each run names its shared memory segments with this prefix, its process id and a random suffix, so runs do not
# collide. A run killed with SIGTERM or SIGKILL leaves its pool workers running, and they keep its segments (in
# /dev/shm on Linux) until they are killed; the prefix and pid find them.
SHM_NAME_PREFIX = "thp_"
