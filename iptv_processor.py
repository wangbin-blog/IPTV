import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', default='./output')
    parser.add_argument('--max-threads', type=int, default=8)
    parser.add_argument('--timeout', type=int, default=10)
    parser.add_argument('--force-refresh', action='store_true')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    processor = IPTVProcessor(config={
        'max_fetch_threads': args.max_threads,
        'request_timeout': args.timeout
    })
    processor.run()
