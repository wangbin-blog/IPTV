import random
import requests
from lxml import etree
import os
import time
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import logging.handlers
from retrying import retry
from dotenv import load_dotenv
from urllib.parse import urlparse

# 加载环境变量
load_dotenv()

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('tv_search.log', encoding='utf-8'),
        logging.handlers.RotatingFileHandler(
            'tv_search_debug.log',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)

class TVSearchCrawler:
    def __init__(self, speed_threshold=1.0, max_workers=3):
        self.speed_threshold = float(speed_threshold)
        self.max_workers = max_workers
        self.current_directory = os.getcwd()
        self.output_file_path = os.path.join(self.current_directory, 'live.txt')
        
        # 用户代理列表
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.179 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_3) AppleWebKit/537.36 (KHTML, like Gecko) Version/15.6 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.179 Safari/537.36',
        ]
        
        # 代理配置
        self.proxies = self._init_proxies()
        
        # 请求延迟
        self.request_delays = [1, 2, 3]
        
        # 搜索源配置
        self.search_sources = [
            {'name': 'tonkiang', 'url': 'http://tonkiang.us/'},
            {'name': 'iptv', 'url': 'http://example.iptvsearch.com/'}
        ]
        
        self.setup_output_file()
    
    def _init_proxies(self):
        """初始化代理配置"""
        proxies = []
        # 从环境变量获取代理
        env_proxy = os.getenv('HTTP_PROXY')
        if env_proxy:
            proxies.append(env_proxy)
        
        # 添加备用代理
        proxies.extend([
            'http://proxy1.example.com:8080',
            'http://proxy2.example.com:8080'
        ])
        return proxies
    
    def setup_output_file(self):
        """初始化输出文件"""
        with open(self.output_file_path, 'w', encoding='utf-8') as f:
            f.write('# TV Search 自动生成的直播源文件\n')
            f.write('# 更新时间: {}\n'.format(time.strftime('%Y-%m-%d %H:%M:%S')))
            f.write('# 速度阈值: {} MB/s\n'.format(self.speed_threshold))
            f.write('# 生成工具: Tv_search.py\n\n')
    
    def setup_driver(self):
        """配置Chrome浏览器驱动"""
        user_agent = random.choice(self.user_agents)
        proxy = random.choice(self.proxies) if self.proxies else None
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={user_agent}")
        
        if proxy:
            chrome_options.add_argument(f'--proxy-server={proxy}')
        
        # GitHub Actions 环境特殊配置
        chrome_options.binary_location = "/usr/bin/google-chrome"
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    
    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def search_tv_channels(self, name):
        """搜索指定频道名称的M3U8链接"""
        all_m3u8 = []
        for source in self.search_sources:
            try:
                logger.info(f"🔍 在 {source['name']} 搜索频道: {name}")
                m3u8_list = self._search_single_source(source['url'], name)
                all_m3u8.extend(m3u8_list)
                time.sleep(random.choice(self.request_delays))
            except Exception as e:
                logger.error(f"❌ 在 {source['name']} 搜索失败: {e}")
                continue
        return all_m3u8
    
    def _search_single_source(self, url, name):
        """在单个源搜索频道"""
        driver = self.setup_driver()
        m3u8_list = []
        
        try:
            driver.get(url)
            
            # 等待搜索框加载
            search_input = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, 'search'))
            )
            search_input.clear()
            search_input.send_keys(name)
            
            # 点击搜索按钮
            submit_button = driver.find_element(By.NAME, 'Submit')
            submit_button.click()
            
            # 等待结果加载
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'resultplus'))
            )
            
            # 解析页面获取M3U8链接
            page_source = driver.page_source
            root = etree.HTML(page_source)
            result_divs = root.xpath("//div[@class='resultplus']")
            
            logger.info(f"📺 频道 '{name}' 找到 {len(result_divs)} 个结果")
            
            for div in result_divs:
                for element in div.xpath(".//tba"):
                    if element.text and element.text.strip():
                        url = element.text.strip()
                        if url.startswith('http') and 'm3u8' in url:
                            m3u8_list.append(url)
                            logger.debug(f"✅ 找到有效链接: {url}")
                            
        except Exception as e:
            logger.error(f"❌ 搜索频道 '{name}' 时出错: {e}")
            raise
        finally:
            driver.quit()
            
        return m3u8_list
    
    def test_stream_quality(self, url, name):
        """测试直播流质量和速度"""
        try:
            logger.info(f"🧪 测试直播流: {name}")
            
            # 首次连接测试
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # 内容类型检查
            content_type = response.headers.get('content-type', '')
            if 'application/x-mpegurl' not in content_type and '#EXTM3U' not in response.text:
                logger.debug(f"⚠️ 非M3U8格式: {url}")
                return None
            
            # 速度测试
            download_speed = self.measure_download_speed(url, response.text)
            if not download_speed or download_speed < self.speed_threshold:
                logger.debug(f"🐌 频道 {name} 速度过慢: {download_speed:.2f} MB/s")
                return None
            
            # 二次验证确保稳定性
            try:
                response = requests.get(url, timeout=5)
                if response.status_code != 200:
                    return None
            except:
                return None
            
            logger.info(f"🎯 频道 {name} 通过所有检查: {download_speed:.2f} MB/s")
            return url
        except Exception as e:
            logger.debug(f"🔴 流测试失败 {url}: {e}")
            return None
    
    def measure_download_speed(self, base_url, m3u8_content):
        """测量下载速度"""
        try:
            lines = m3u8_content.split('\n')
            segments = [line.strip() for line in lines if line and not line.startswith('#')]
            
            if not segments:
                return None
            
            # 测试前3个片段取平均值
            test_segments = segments[:3]
            total_speed = 0
            valid_tests = 0
            
            for segment in test_segments:
                if not segment.startswith('http'):
                    segment = base_url.rsplit('/', 1)[0] + '/' + segment
                
                try:
                    start_time = time.time()
                    response = requests.get(segment, timeout=10, stream=True)
                    content = response.content
                    end_time = time.time()
                    
                    if response.status_code == 200:
                        download_time = end_time - start_time
                        file_size = len(content)
                        speed = file_size / download_time / (1024 * 1024)  # MB/s
                        total_speed += speed
                        valid_tests += 1
                except Exception:
                    continue
            
            return total_speed / valid_tests if valid_tests > 0 else None
        except Exception as e:
            logger.debug(f"⏱️ 速度测量失败: {e}")
            return None
    
    def process_tv_category(self, category_name):
        """处理一个电视频道分类"""
        category_file = f'{category_name}.txt'
        if not os.path.exists(category_file):
            logger.warning(f"📄 频道文件不存在: {category_file}")
            return
        
        # 读取频道列表
        with open(category_file, 'r', encoding='utf-8') as f:
            channel_names = [line.strip() for line in f if line.strip()]
        
        logger.info(f"🎬 处理电视分类 '{category_name}', 共 {len(channel_names)} 个频道")
        
        # 写入分类标题
        with open(self.output_file_path, 'a', encoding='utf-8') as f:
            f.write(f'\n{category_name},#genre#\n')
        
        valid_count = 0
        
        # 处理每个频道
        for channel_name in channel_names:
            logger.info(f"📡 处理频道: {channel_name}")
            
            # 搜索M3U8链接
            m3u8_urls = self.search_tv_channels(channel_name)
            if not m3u8_urls:
                logger.warning(f"🔍 未找到频道 {channel_name} 的链接")
                continue
            
            # 测试链接质量
            valid_urls = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {
                    executor.submit(self.test_stream_quality, url, channel_name): url 
                    for url in m3u8_urls[:5]  # 限制测试数量避免超时
                }
                
                for future in as_completed(future_to_url):
                    result = future.result()
                    if result:
                        valid_urls.append(result)
            
            # 保存有效链接
            if valid_urls:
                with open(self.output_file_path, 'a', encoding='utf-8') as f:
                    for url in valid_urls:
                        f.write(f'{channel_name},{url}\n')
                valid_count += len(valid_urls)
                logger.info(f"✅ 频道 '{channel_name}' 找到 {len(valid_urls)} 个有效链接")
            else:
                logger.warning(f"❌ 频道 '{channel_name}' 无有效链接")
            
            time.sleep(random.choice(self.request_delays))  # 随机延迟避免请求过于频繁
        
        logger.info(f"🎉 电视分类 '{category_name}' 完成，共找到 {valid_count} 个有效链接")
    
    def remove_duplicate_streams(self):
        """去除重复的直播源"""
        if not os.path.exists(self.output_file_path):
            return
        
        with open(self.output_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 分离文件头和信息行
        header = []
        content_lines = []
        seen_urls = set()
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#') or line.endswith('#genre#'):
                header.append(line + '\n')
            else:
                parts = line.split(',', 1)
                if len(parts) == 2:
                    channel, url = parts[0].strip(), parts[1].strip()
                    parsed_url = urlparse(url)
                    clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                    if clean_url not in seen_urls:
                        seen_urls.add(clean_url)
                        content_lines.append(line + '\n')
        
        # 重新写入文件
        with open(self.output_file_path, 'w', encoding='utf-8') as f:
            f.writelines(header)
            f.writelines(content_lines)
        
        logger.info(f"🔄 去重完成，剩余 {len(content_lines)} 个唯一直播源")
    
    def cleanup_old_streams(self, days=7):
        """清理过期的直播源"""
        # 实现基于时间戳的清理逻辑
        # 可以扩展为从文件内容中解析出时间信息
        pass
    
    def run_tv_search(self, categories=None):
        """运行TV搜索主程序"""
        if categories is None:
            categories = ['央视频道']
        elif isinstance(categories, str):
            categories = [cat.strip() for cat in categories.split(',')]
        
        logger.info("🚀 开始TV搜索直播源爬虫")
        logger.info(f"⚡ 速度阈值: {self.speed_threshold} MB/s")
        logger.info(f"📺 处理分类: {categories}")
        
        start_time = time.time()
        total_valid_streams = 0
        
        for category in categories:
            self.process_tv_category(category)
        
        # 去重处理
        self.remove_duplicate_streams()
        
        # 统计最终结果
        if os.path.exists(self.output_file_path):
            with open(self.output_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                content_lines = [line for line in lines if line.strip() and not line.startswith('#') and not line.endswith('#genre#\n')]
                total_valid_streams = len(content_lines)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info("🎊 TV搜索完成！")
        logger.info(f"⏰ 总耗时: {execution_time:.2f} 秒")
        logger.info(f"📊 总有效直播源: {total_valid_streams} 个")
        
        return total_valid_streams

def main():
    """主函数"""
    try:
        # 从环境变量获取配置
        speed_threshold = os.getenv('SPEED_THRESHOLD', '1.0')
        categories = os.getenv('CATEGORIES', '央视频道')
        
        # 创建TV搜索实例
        tv_crawler = TVSearchCrawler(
            speed_threshold=speed_threshold, 
            max_workers=3
        )
        
        # 运行搜索
        total_streams = tv_crawler.run_tv_search(categories)
        
        # 清理旧数据
        tv_crawler.cleanup_old_streams()
        
        # 输出结果摘要
        print(f"\n{'='*50}")
        print(f"TV搜索完成摘要:")
        print(f"  速度阈值: {speed_threshold} MB/s")
        print(f"  处理分类: {categories}")
        print(f"  有效直播源: {total_streams} 个")
        print(f"  输出文件: live.txt")
        print(f"{'='*50}")
        
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"💥 TV搜索程序异常: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
