import time

from core.config import load_config
from ingestion.services.collector_service import CollectorService


def main():
    config = load_config()
    collector_service = CollectorService(config=config)
    logger = collector_service.logger

    logger.info("自动采集程序启动")
    print("自动采集程序启动")

    while True:
        start_time = time.time()

        try:
            print("开始执行一轮采集...")
            collector_service.collect_once()
            print("本轮采集完成")
        except Exception as error:
            logger.exception(f"本轮采集执行失败: {error}")
            print(f"本轮采集执行失败: {error}")

        elapsed = time.time() - start_time
        sleep_time = max(0, config.collect.interval_seconds - elapsed)

        logger.info(f"本轮总耗时 {elapsed:.2f} 秒，休眠 {sleep_time:.2f} 秒")
        print(f"本轮总耗时 {elapsed:.2f} 秒，休眠 {sleep_time:.2f} 秒")

        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
