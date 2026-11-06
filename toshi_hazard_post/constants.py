"""Names of shared memory objects."""

# Each run names its shared memory segments with this prefix, its process id and a random suffix, so runs do not
# collide. A run killed with SIGKILL leaves its segments behind (in /dev/shm on Linux); the prefix finds them.
SHM_NAME_PREFIX = "thp_"
