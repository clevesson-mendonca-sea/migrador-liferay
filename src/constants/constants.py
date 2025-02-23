BATCH_SIZE = 25
MAX_CONCURRENT_TASKS = 50
THREAD_POOL_SIZE = 10
CONNECTION_POOL_SIZE = 100
SEMAPHORE_LIMIT = 20

HTTP_TIMEOUT = {
    'total': 180,
    'connect': 30,
    'sock_read': 60,
    'sock_connect': 30
}

# Cache settings
DNS_CACHE_TTL = 1200  # 20 minutos
KEEPALIVE_TIMEOUT = 120  # 2 minutos