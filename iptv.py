#!/usr/bin/env python3
"""
🎬 IPTV智能管理工具 - GitHub Actions 优化版 v6.1
流程：智能抓取 → 精准测速 → 模板匹配 → 生成文件
特点：优化抓取策略 + 精准测速算法 + 智能过滤机制 + 全面质量控制
"""

import requests
import pandas as pd
import re
import os
import time
import subprocess
import sys
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from pathlib import Path
import shutil
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any, Optional, Tuple, Union, Callable
from contextlib import contextmanager
import signal


# ==================== 核心数据类型 ====================

class StreamType(Enum):
    """流媒体类型枚举"""
    M3U8 = "m3u8"
    TS = "ts"
    FLV = "flv"
    MP4 = "mp4"
    RTMP = "rtmp"
    RTSP = "rtsp"
    UNKNOWN = "unknown"


@dataclass
class StreamInfo:
    """流信息数据类"""
    program_name: str
    stream_url: str
    group: str = "默认分组"
    original_name: str = ""
    match_score: int = 0
    accessible: bool = False
    speed: float = float('inf')
    stream_type: StreamType = StreamType.UNKNOWN
    last_tested: float = 0
    content_type: str = ""
    file_size: int = 0


@dataclass
class SpeedTestResult:
    """测速结果数据类"""
    url: str
    accessible: bool = False
    speed: float = float('inf')
    stream_type: StreamType = StreamType.UNKNOWN
    error_message: str = ""
    content_type: str = ""
    file_size: int = 0
    response_code: int = 0
    last_tested: float = 0


@dataclass
class ProcessingStats:
    """处理统计信息"""
    sources_fetched: int = 0
    streams_parsed: int = 0
    channels_matched: int = 0
    sources_tested: int = 0
    sources_available: int = 0
    errors_encountered: int = 0
    categories_processed: int = 0
    channels_with_sources: int = 0
    total_sources_found: int = 0
    quality_filtered: int = 0
    speed_filtered: int = 0


# ==================== 配置管理系统 ====================

class GitHubConfigManager:
    """GitHub Actions 专用配置管理器"""
    
    def __init__(self):
        # 文件配置
        self.template_file: str = "demo.txt"
        self.output_txt: str = "iptv.txt"
        self.output_m3u: str = "iptv.m3u"
        self.temp_dir: str = "temp"
        self.cache_dir: str = "cache"
        self.backup_dir: str = "backup"
        
        # GitHub环境优化配置
        self.request_timeout: int = 12
        self.request_retries: int = 2
        self.max_workers: int = 8  # GitHub Actions限制
        self.connection_pool_size: int = 10
        
        # 智能抓取配置
        self.enable_smart_crawling: bool = True
        self.crawling_batch_size: int = 5
        self.source_priority: Dict[str, int] = {
            "github.com": 10,
            "raw.githubusercontent.com": 9,
            "gitee.com": 8,
            "mirror.ghproxy.com": 7
        }
        
        # 测速配置 - GitHub环境优化
        self.open_speed_test: bool = True
        self.speed_test_limit: int = 6  # 减少并发避免限制
        self.speed_test_timeout: int = 8
        self.enable_smart_speed_test: bool = True
        self.speed_test_strategy: str = "conservative"  # GitHub环境使用保守策略
        
        # 过滤配置
        self.open_filter_speed: bool = True
        self.min_speed: float = 0.5
        self.max_speed: float = 12.0
        self.enable_quality_filter: bool = True
        self.min_content_length: int = 1024
        self.max_content_length: int = 5242880  # 5MB
        
        # 内容类型过滤
        self.allowed_content_types: List[str] = [
            "video/", "audio/", "application/", "text/",
            "octet-stream", "x-mpegurl", "mpegurl"
        ]
        self.blocked_content_types: List[str] = [
            "text/html", "application/json", "text/plain"
        ]
        
        # 域名过滤
        self.blocked_domains: List[str] = [
            "example.com", "localhost", "127.0.0.1",
            "test.com", "dummy.com"
        ]
        
        # 匹配配置
        self.similarity_threshold: int = 50
        self.max_sources_per_channel: int = 5  # 减少源数量
        self.enable_fuzzy_matching: bool = True
        self.matching_confidence: float = 0.7
        
        # 质量控制
        self.enable_quality_control: bool = True
        self.min_stream_size: int = 512
        self.max_url_length: int = 350
        
        # 性能优化
        self.enable_caching: bool = True
        self.cache_ttl: int = 1800  # 30分钟缓存
        self.enable_compression: bool = True
        
        # 显示配置 - GitHub环境减少输出
        self.progress_bar_width: int = 30
        self.show_detailed_stats: bool = True
        self.enable_real_time_stats: bool = False  # GitHub Actions中关闭实时统计
        
        # 优化的源URL列表 - 选择稳定性高的源
        self.source_urls: List[str] = [
            # 高稳定性源
            "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/global.m3u",
            "https://iptv-org.github.io/iptv/index.nsfw.m3u",
            "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/cn.m3u",
            
            # 备用源
            "https://mirror.ghproxy.com/https://raw.githubusercontent.com/zhanghongchen/iptv/main/直播.txt",
            "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
        ]
        
        # HTTP请求头配置
        self.headers: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 GitHub-Actions-IPTV',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        }


# ==================== 进度显示管理器 ====================

class ProgressDisplay:
    """进度显示管理器 - GitHub Actions优化版本"""
    
    def __init__(self):
        self.start_time: Optional[float] = None
        self.current_step: int = 0
        self.total_steps: int = 0
        self.step_names: List[str] = []
        self.step_start_time: Optional[float] = None
    
    def start_progress(self, step_names: List[str]) -> None:
        """开始进度跟踪"""
        self.step_names = step_names
        self.total_steps = len(step_names)
        self.current_step = 0
        self.start_time = time.time()
        self._print_header()
    
    def next_step(self, message: str = "") -> None:
        """进入下一步"""
        if self.step_start_time:
            step_time = time.time() - self.step_start_time
            logging.info(f"步骤 {self.current_step} 耗时: {step_time:.2f}秒")
        
        self.current_step += 1
        if self.current_step <= self.total_steps:
            step_name = self.step_names[self.current_step - 1]
            self.step_start_time = time.time()
            self._print_step(step_name, message)
    
    def update_substep(self, message: str, symbol: str = "🔹") -> None:
        """更新子步骤进度"""
        elapsed = time.time() - (self.start_time or time.time())
        print(f"  {symbol} [{elapsed:6.1f}s] {message}")
    
    def _print_header(self) -> None:
        """打印进度头"""
        print("\n" + "="*60)
        print("🎬 IPTV智能管理工具 - GitHub Actions优化版 v6.1")
        print("🔧 流程: 智能抓取 → 精准测速 → 模板匹配 → 生成文件")
        print("="*60)
    
    def _print_step(self, step_name: str, message: str) -> None:
        """打印步骤信息"""
        elapsed = time.time() - (self.start_time or time.time())
        print(f"\n📋 步骤 {self.current_step}/{self.total_steps}: {step_name}")
        if message:
            print(f"   📝 {message}")
        print(f"   ⏰ 总用时: {elapsed:.1f}秒")


# ==================== 智能测速引擎 ====================

class SmartSpeedTestEngine:
    """智能测速引擎核心类 - GitHub Actions优化版"""
    
    def __init__(self, config: GitHubConfigManager):
        self.config = config
        self.session = self._create_session()
        self._stop_event = threading.Event()
        self._patterns = self._compile_patterns()
        self._cache: Dict[str, SpeedTestResult] = {}
        self._stats = {
            'total_tests': 0,
            'successful_tests': 0,
            'failed_tests': 0,
            'average_speed': 0.0
        }
    
    def _create_session(self) -> requests.Session:
        """创建优化的HTTP会话"""
        session = requests.Session()
        session.headers.update(self.config.headers)
        
        # 优化连接适配器
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.config.connection_pool_size,
            pool_maxsize=self.config.connection_pool_size,
            max_retries=2,
            pool_block=False
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """编译正则表达式模式"""
        return {
            'stream_protocol': re.compile(r'^(https?|rtmp|rtsp)://', re.IGNORECASE),
            'domain_extract': re.compile(r'://([^/]+)')
        }
    
    def stop(self) -> None:
        """停止测速"""
        self._stop_event.set()
        self.session.close()
    
    def _detect_stream_type(self, url: str) -> StreamType:
        """智能检测流媒体类型"""
        if not url:
            return StreamType.UNKNOWN
        
        url_lower = url.lower()
        
        # 精确匹配流媒体类型
        if '.m3u8' in url_lower or 'm3u8' in url_lower:
            return StreamType.M3U8
        elif '.ts' in url_lower or 'ts' in url_lower:
            return StreamType.TS
        elif '.flv' in url_lower or 'flv' in url_lower:
            return StreamType.FLV
        elif '.mp4' in url_lower or 'mp4' in url_lower:
            return StreamType.MP4
        elif url_lower.startswith('rtmp://'):
            return StreamType.RTMP
        elif url_lower.startswith('rtsp://'):
            return StreamType.RTSP
        else:
            # 基于内容类型推断
            return StreamType.UNKNOWN
    
    def _is_blocked_domain(self, url: str) -> bool:
        """检查是否为被阻止的域名"""
        try:
            domain_match = self._patterns['domain_extract'].search(url)
            if domain_match:
                domain = domain_match.group(1).lower()
                for blocked in self.config.blocked_domains:
                    if blocked in domain:
                        return True
        except Exception:
            pass
        return False
    
    def _is_allowed_content_type(self, content_type: str) -> bool:
        """检查内容类型是否允许"""
        if not content_type:
            return True
            
        content_type_lower = content_type.lower()
        
        # 检查阻止的内容类型
        for blocked_type in self.config.blocked_content_types:
            if blocked_type in content_type_lower:
                return False
        
        # 检查允许的内容类型
        for allowed_type in self.config.allowed_content_types:
            if allowed_type in content_type_lower:
                return True
        
        return False
    
    def _adaptive_speed_test(self, url: str) -> Tuple[bool, float, str, int]:
        """自适应测速策略"""
        try:
            start_time = time.time()
            
            # 根据URL类型选择测速策略
            if any(proto in url.lower() for proto in ['.m3u8', '.ts', '.flv']):
                # 流媒体使用HEAD请求快速检测
                response = self.session.head(
                    url, 
                    timeout=self.config.speed_test_timeout,
                    allow_redirects=True
                )
            else:
                # 其他类型使用GET请求部分内容
                response = self.session.get(
                    url,
                    timeout=self.config.speed_test_timeout,
                    allow_redirects=True,
                    stream=True
                )
            
            end_time = time.time()
            response_time = end_time - start_time
            
            # 检查响应状态
            if response.status_code in [200, 206, 302, 301, 307]:
                content_type = response.headers.get('Content-Type', '')
                content_length = int(response.headers.get('Content-Length', 0))
                
                # 验证内容类型
                if not self._is_allowed_content_type(content_type):
                    response.close()
                    return False, float('inf'), content_type, content_length
                
                response.close()
                return True, response_time, content_type, content_length
            else:
                response.close()
                return False, float('inf'), '', 0
                
        except requests.exceptions.Timeout:
            return False, float('inf'), '', 0
        except requests.exceptions.ConnectionError:
            return False, float('inf'), '', 0
        except requests.exceptions.RequestException as e:
            logging.debug(f"测速请求异常: {url} - {e}")
            return False, float('inf'), '', 0
        except Exception as e:
            logging.debug(f"测速未知异常: {url} - {e}")
            return False, float('inf'), '', 0
    
    def test_single_url(self, url: str) -> SpeedTestResult:
        """测试单个URL - 智能优化版本"""
        if self._stop_event.is_set():
            return SpeedTestResult(url=url, accessible=False)
        
        # 检查缓存
        cache_key = f"test_{hash(url) & 0xFFFFFFFF}"
        if self.config.enable_caching and cache_key in self._cache:
            cached_result = self._cache[cache_key]
            if time.time() - cached_result.last_tested < self.config.cache_ttl:
                return cached_result
        
        result = SpeedTestResult(url=url)
        self._stats['total_tests'] += 1
        
        # 检查被阻止的域名
        if self._is_blocked_domain(url):
            result.accessible = False
            result.error_message = "域名被阻止"
            return result
        
        try:
            # 执行自适应测速
            accessible, speed, content_type, file_size = self._adaptive_speed_test(url)
            
            result.accessible = accessible
            result.speed = speed
            result.content_type = content_type
            result.file_size = file_size
            result.stream_type = self._detect_stream_type(url)
            result.last_tested = time.time()
            
            if accessible:
                self._stats['successful_tests'] += 1
                # 更新平均速度
                total_speed = self._stats['average_speed'] * (self._stats['successful_tests'] - 1)
                self._stats['average_speed'] = (total_speed + speed) / self._stats['successful_tests']
            else:
                self._stats['failed_tests'] += 1
                result.error_message = "测速失败"
                
        except Exception as e:
            result.accessible = False
            result.error_message = str(e)
            self._stats['failed_tests'] += 1
        
        # 缓存结果
        if self.config.enable_caching:
            self._cache[cache_key] = result
        
        return result
    
    def batch_speed_test(self, urls: List[str], 
                        progress_callback: Callable = None) -> Dict[str, SpeedTestResult]:
        """批量测速 - 智能优化版本"""
        if not self.config.open_speed_test:
            # 如果测速关闭，返回所有URL为可访问
            return {url: SpeedTestResult(url=url, accessible=True) for url in urls}
        
        self._stop_event.clear()
        results = {}
        
        logging.info(f"🚀 开始批量测速，共 {len(urls)} 个URL，并发数: {self.config.speed_test_limit}")
        
        def test_with_callback(url: str) -> Tuple[str, SpeedTestResult]:
            if self._stop_event.is_set():
                return url, SpeedTestResult(url=url, accessible=False)
            
            result = self.test_single_url(url)
            if progress_callback:
                progress_callback(url, result)
            return url, result
        
        try:
            # 智能分批测速
            with ThreadPoolExecutor(max_workers=self.config.speed_test_limit) as executor:
                # GitHub环境使用保守策略
                batch_size = 20
                
                total_urls = len(urls)
                
                for i in range(0, total_urls, batch_size):
                    if self._stop_event.is_set():
                        break
                        
                    batch_urls = urls[i:i + batch_size]
                    future_to_url = {
                        executor.submit(test_with_callback, url): url 
                        for url in batch_urls
                    }
                    
                    for future in as_completed(future_to_url):
                        if self._stop_event.is_set():
                            break
                        try:
                            url, result = future.result(timeout=self.config.speed_test_timeout + 10)
                            results[url] = result
                        except Exception as e:
                            url = future_to_url[future]
                            results[url] = SpeedTestResult(
                                url=url, 
                                accessible=False, 
                                error_message=str(e)
                            )
                        
        except Exception as e:
            logging.error(f"❌ 批量测速失败: {e}")
        
        # 输出测速统计
        success_rate = (self._stats['successful_tests'] / self._stats['total_tests'] * 100) if self._stats['total_tests'] > 0 else 0
        logging.info(f"📊 测速统计: 成功 {self._stats['successful_tests']}/{self._stats['total_tests']} ({success_rate:.1f}%)，平均速度: {self._stats['average_speed']:.2f}s")
        
        return results


# ==================== IPTV智能管理器 ====================

class IPTVManager:
    """IPTV智能管理工具核心类 - GitHub Actions优化版"""
    
    def __init__(self, config: GitHubConfigManager = None) -> None:
        # 检查是否在GitHub Actions环境中
        if os.getenv('GITHUB_ACTIONS'):
            self.config: GitHubConfigManager = GitHubConfigManager()
            print("🏃 检测到GitHub Actions环境，使用优化配置")
        else:
            self.config: GitHubConfigManager = config or GitHubConfigManager()
            
        self.stats: ProcessingStats = ProcessingStats()
        self.progress: ProgressDisplay = ProgressDisplay()
        self.speed_engine: SmartSpeedTestEngine = SmartSpeedTestEngine(self.config)
        self._is_running: bool = True
        self._patterns: Dict[str, re.Pattern] = self._compile_patterns()
        self._setup_environment()
        self._setup_signal_handlers()
        
    def _setup_environment(self) -> None:
        """设置运行环境"""
        try:
            directories = [self.config.temp_dir, self.config.cache_dir, self.config.backup_dir]
            for directory in directories:
                Path(directory).mkdir(exist_ok=True)
            logging.info("✅ 环境设置完成")
        except Exception as e:
            logging.error(f"❌ 环境设置失败: {e}")
            raise

    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """编译正则表达式模式"""
        return {
            'extinf': re.compile(r'#EXTINF:.*?tvg-name="([^"]+)".*?,(.+)', re.IGNORECASE),
            'category': re.compile(r'^(.*?),#genre#$', re.IGNORECASE),
            'url': re.compile(r'https?://[^\s,]+', re.IGNORECASE),
            'tvg_name': re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE),
            'tvg_id': re.compile(r'tvg-id="([^"]*)"', re.IGNORECASE),
            'group_title': re.compile(r'group-title="([^"]*)"', re.IGNORECASE),
            'extinf_content': re.compile(r',\s*(.+)$', re.IGNORECASE),
            'channel_code': re.compile(r'([A-Z]+)-?(\d+)', re.IGNORECASE),
            'quality_suffix': re.compile(r'\s+(HD|FHD|4K|8K|高清|超清|直播|LIVE|频道|TV)', re.IGNORECASE),
            'brackets': re.compile(r'[\[\(\{].*?[\]\)\}]'),
            'whitespace': re.compile(r'\s+'),
            'special_chars': re.compile(r'[^\w\u4e00-\u9fa5\s-]'),
            'domain_extract': re.compile(r'://([^/:]+)')
        }

    def _setup_signal_handlers(self) -> None:
        """设置信号处理器"""
        def signal_handler(signum, frame):
            logging.info(f"🛑 收到信号 {signum}，正在优雅退出...")
            self._is_running = False
            self.cleanup()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _print_progress_bar(self, current: int, total: int, prefix: str = "", suffix: str = "") -> None:
        """打印进度条"""
        if total == 0:
            return
            
        percent = current / total
        filled_length = int(self.config.progress_bar_width * percent)
        bar = '█' * filled_length + '░' * (self.config.progress_bar_width - filled_length)
        percent_display = f"{percent:.1%}"
        
        print(f"\r{prefix} |{bar}| {current}/{total} {percent_display} {suffix}", end='', flush=True)
        
        if current == total:
            print()

    def validate_url(self, url: str) -> bool:
        """验证URL格式和安全性 - 增强版本"""
        if not url or not isinstance(url, str) or len(url) > self.config.max_url_length:
            return False
        
        try:
            result = urlparse(url)
            if not all([result.scheme, result.netloc]):
                return False
            
            # 检查协议
            if result.scheme not in ['http', 'https', 'rtmp', 'rtsp']:
                return False
            
            # 检查路径安全性
            if any(char in result.path for char in ['//', '\\', '../']):
                return False
            
            # 检查被阻止的域名
            domain = result.netloc.lower()
            for blocked in self.config.blocked_domains:
                if blocked in domain:
                    return False
            
            return True
            
        except Exception:
            return False

    def _get_url_priority(self, url: str) -> int:
        """获取URL优先级"""
        for domain, priority in self.config.source_priority.items():
            if domain in url:
                return priority
        return 5  # 默认优先级

    @contextmanager
    def _request_context(self, url: str, timeout: int = None):
        """请求上下文管理器"""
        timeout = timeout or self.config.request_timeout
        start_time = time.time()
        response = None
        try:
            response = self.speed_engine.session.get(
                url, 
                timeout=timeout, 
                stream=True, 
                allow_redirects=True
            )
            yield response
        finally:
            if response:
                response.close()
            elapsed = time.time() - start_time
            logging.debug(f"请求 {url} 耗时: {elapsed:.2f}秒")

    def fetch_streams_from_url(self, url: str) -> Optional[str]:
        """从URL获取流数据 - 智能优化版本"""
        if not self.validate_url(url):
            logging.debug(f"❌ 无效的URL: {url}")
            return None
        
        # 根据优先级调整超时时间
        priority = self._get_url_priority(url)
        base_timeout = max(5, self.config.request_timeout - (priority - 5))
        
        for attempt in range(self.config.request_retries):
            if not self._is_running:
                return None
            try:
                # 智能超时调整
                timeout = base_timeout + (attempt * 5)
                with self._request_context(url, timeout) as response:
                    if response.status_code == 200:
                        # 流式读取，内存优化
                        content_chunks = []
                        total_size = 0
                        for chunk in response.iter_content(chunk_size=16384):  # 增大块大小
                            if not self._is_running:
                                return None
                            content_chunks.append(chunk)
                            total_size += len(chunk)
                            # 智能大小控制
                            if total_size > self.config.max_content_length:
                                logging.info(f"📦 内容过大({total_size}字节)，截断处理: {url}")
                                break
                        
                        content = b''.join(content_chunks).decode('utf-8', errors='ignore')
                        if len(content) >= self.config.min_stream_size:
                            self.stats.sources_fetched += 1
                            logging.debug(f"✅ 成功抓取: {url} ({len(content)}字节)")
                            return content
                        else:
                            logging.debug(f"📝 内容过小: {url} ({len(content)}字节)")
                            return None
                    elif response.status_code == 429:  # 频率限制
                        wait_time = (attempt + 1) * 10
                        logging.info(f"⏳ 频率限制，等待 {wait_time} 秒: {url}")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code >= 500:  # 服务器错误
                        logging.warning(f"🔧 服务器错误 {response.status_code}，重试: {url}")
                        time.sleep((attempt + 1) * 3)
                        continue
                    else:
                        logging.debug(f"❌ HTTP {response.status_code}: {url}")
                        return None
            except requests.exceptions.Timeout:
                logging.debug(f"⏰ 请求超时 (尝试 {attempt + 1}): {url}")
                if attempt < self.config.request_retries - 1:
                    time.sleep((attempt + 1) * 2)
                continue
            except requests.exceptions.ConnectionError:
                logging.debug(f"🔌 连接错误 (尝试 {attempt + 1}): {url}")
                if attempt < self.config.request_retries - 1:
                    time.sleep((attempt + 1) * 3)
                continue
            except Exception as e:
                logging.debug(f"❌ 请求失败 (尝试 {attempt + 1}): {url} - {e}")
                if attempt < self.config.request_retries - 1:
                    time.sleep((attempt + 1) * 2)
        return None

    def fetch_all_streams(self) -> str:
        """获取所有源的流数据 - 智能优化版本"""
        self.progress.update_substep("启动智能多源抓取...", "🌐")
        
        if not self.config.source_urls:
            logging.error("❌ 没有配置源URL")
            return ""
        
        # 按优先级排序URL
        sorted_urls = sorted(
            self.config.source_urls,
            key=self._get_url_priority,
            reverse=True
        )
        
        all_streams: List[str] = []
        successful_sources = 0
        
        print("   抓取进度: ", end="", flush=True)
        
        try:
            # 智能分批抓取
            with ThreadPoolExecutor(max_workers=min(self.config.max_workers, len(sorted_urls))) as executor:
                # 分批处理，避免内存峰值
                batch_size = self.config.crawling_batch_size
                
                for batch_start in range(0, len(sorted_urls), batch_size):
                    if not self._is_running:
                        break
                        
                    batch_urls = sorted_urls[batch_start:batch_start + batch_size]
                    future_to_url = {
                        executor.submit(self.fetch_streams_from_url, url): url 
                        for url in batch_urls
                    }
                    
                    for future in as_completed(future_to_url):
                        if not self._is_running:
                            break
                        url = future_to_url[future]
                        try:
                            content = future.result(timeout=self.config.request_timeout + 20)
                            if content:
                                all_streams.append(content)
                                successful_sources += 1
                                print("✅", end="", flush=True)
                            else:
                                print("❌", end="", flush=True)
                        except Exception as e:
                            logging.debug(f"抓取失败: {url} - {e}")
                            print("💥", end="", flush=True)
                        
                        # 实时进度更新
                        current_total = batch_start + len(future_to_url)
                        self._print_progress_bar(
                            current_total, 
                            len(sorted_urls), 
                            "   抓取进度", 
                            f"{successful_sources}成功"
                        )
        
        except Exception as e:
            logging.error(f"❌ 并发获取失败: {e}")
            return ""
        
        print()
        total_content = "\n".join(all_streams)
        
        # 内容去重和优化
        if self.config.enable_compression:
            lines = total_content.splitlines()
            unique_lines = list(dict.fromkeys(lines))  # 保持顺序的去重
            total_content = "\n".join(unique_lines)
        
        self.progress.update_substep(
            f"抓取完成: {successful_sources}/{len(sorted_urls)} 个源, " 
            f"总数据: {len(total_content)} 字符, "
            f"去重后: {len(total_content.splitlines())} 行", 
            "✅"
        )
        
        return total_content

    def _extract_program_name(self, extinf_line: str) -> str:
        """从EXTINF行提取节目名称 - 增强版本"""
        if not extinf_line.startswith('#EXTINF'):
            return "未知频道"
        try:
            # 优先使用tvg-name
            tvg_match = self._patterns['tvg_name'].search(extinf_line)
            if tvg_match and tvg_match.group(1).strip():
                return tvg_match.group(1).strip()
            
            # 其次使用逗号后的内容
            content_match = self._patterns['extinf_content'].search(extinf_line)
            if content_match and content_match.group(1).strip():
                name = content_match.group(1).strip()
                # 智能清理名称
                name = self._patterns['brackets'].sub('', name)
                name = self._patterns['quality_suffix'].sub('', name)
                name = self._patterns['whitespace'].sub(' ', name).strip()
                return name if name and name != "未知频道" else "未知频道"
        except Exception as e:
            logging.debug(f"名称提取失败: {extinf_line} - {e}")
        return "未知频道"

    def parse_m3u(self, content: str) -> List[StreamInfo]:
        """解析M3U格式内容 - 优化版本"""
        if not content:
            return []
        
        streams: List[StreamInfo] = []
        lines = content.splitlines()
        current_program: Optional[str] = None
        current_group = "默认分组"
        
        i = 0
        while i < len(lines) and self._is_running:
            line = lines[i].strip()
            if not line:
                i += 1
                continue
                
            if line.startswith("#EXTINF"):
                current_program = self._extract_program_name(line)
                group_match = self._patterns['group_title'].search(line)
                current_group = group_match.group(1).strip() if group_match else "默认分组"
                
                # 查找对应的URL
                j = i + 1
                while j < len(lines):
                    next_line = lines[j].strip()
                    if next_line and not next_line.startswith('#'):
                        if self.validate_url(next_line):
                            streams.append(StreamInfo(
                                program_name=current_program,
                                stream_url=next_line,
                                group=current_group,
                                original_name=current_program
                            ))
                        i = j
                        break
                    j += 1
            elif line.startswith(('http://', 'https://', 'rtmp://', 'rtsp://')):
                # 直接URL行
                if self.validate_url(line):
                    streams.append(StreamInfo(
                        program_name="未知频道",
                        stream_url=line,
                        group="默认分组",
                        original_name="未知频道"
                    ))
            i += 1
        return streams

    def parse_txt(self, content: str) -> List[StreamInfo]:
        """解析TXT格式内容 - 优化版本"""
        if not content:
            return []
        
        streams: List[StreamInfo] = []
        
        for line_num, line in enumerate(content.splitlines(), 1):
            if not self._is_running:
                break
            line = line.strip()
            if not line or line.startswith('#') or '#genre#' in line:
                continue
            
            try:
                # 智能分隔符检测
                separators = [',', ' ', '\t', '|', '$', ';', '：']
                for sep in separators:
                    if sep in line:
                        parts = line.split(sep, 1)
                        if len(parts) == 2:
                            program_name = parts[0].strip()
                            url_part = parts[1].strip()
                            url_match = self._patterns['url'].search(url_part)
                            if url_match and self.validate_url(url_match.group()):
                                streams.append(StreamInfo(
                                    program_name=program_name,
                                    stream_url=url_match.group(),
                                    group="默认分组",
                                    original_name=program_name
                                ))
                                break
                        break
                else:
                    # 没有分隔符，直接查找URL
                    url_match = self._patterns['url'].search(line)
                    if url_match and self.validate_url(url_match.group()):
                        program_name = line.replace(url_match.group(), '').strip()
                        streams.append(StreamInfo(
                            program_name=program_name or "未知频道",
                            stream_url=url_match.group(),
                            group="默认分组",
                            original_name=program_name or "未知频道"
                        ))
            except Exception as e:
                logging.debug(f"解析行失败 {line_num}: {line} - {e}")
                continue
        return streams

    def organize_streams(self, content: str) -> pd.DataFrame:
        """整理流数据 - 第一步：智能解析"""
        self.progress.update_substep("智能解析流数据...", "🔍")
        
        if not content:
            logging.error("❌ 没有内容可处理")
            return pd.DataFrame()
            
        try:
            # 根据内容格式选择解析器
            if content.startswith("#EXTM3U"):
                streams = self.parse_m3u(content)
            else:
                streams = self.parse_txt(content)
            
            if not streams:
                logging.error("❌ 未能解析出任何流数据")
                return pd.DataFrame()
                
            # 转换为DataFrame
            data = []
            for stream in streams:
                data.append({
                    'program_name': stream.program_name,
                    'stream_url': stream.stream_url,
                    'group': stream.group,
                    'original_name': stream.original_name,
                    'stream_type': stream.stream_type.value
                })
            
            df = pd.DataFrame(data)
            self.stats.streams_parsed = len(df)
            
            # 智能数据清理
            initial_count = len(df)
            
            # 移除空值
            df = df.dropna()
            
            # 过滤无效名称和URL
            df = df[df['program_name'].str.len() > 0]
            df = df[df['stream_url'].str.len() > 0]
            
            # URL验证
            df['url_valid'] = df['stream_url'].apply(self.validate_url)
            df = df[df['url_valid']].drop('url_valid', axis=1)
            
            # 智能去重
            df = df.drop_duplicates(subset=['program_name', 'stream_url'], keep='first')
            
            final_count = len(df)
            removed_count = initial_count - final_count
            
            self.progress.update_substep(
                f"解析完成: {initial_count} → {final_count} 个流 "
                f"(移除 {removed_count} 个无效数据)", 
                "✅"
            )
            
            return df
            
        except Exception as e:
            logging.error(f"❌ 数据处理错误: {e}")
            self.stats.errors_encountered += 1
            return pd.DataFrame()

    def speed_test_and_filter(self, sources_df: pd.DataFrame) -> pd.DataFrame:
        """测速和过滤 - 第二步：智能测速"""
        self.progress.update_substep("启动智能测速...", "⏱️")
        
        if sources_df.empty:
            logging.error("❌ 没有需要测速的源")
            return pd.DataFrame()
            
        urls = sources_df['stream_url'].tolist()
        
        # 进度回调函数
        def progress_callback(url: str, result: SpeedTestResult):
            # 实时统计更新
            pass
        
        # 执行批量测速
        results = self.speed_engine.batch_speed_test(urls, progress_callback)
        
        # 处理测速结果
        speed_results = []
        accessible_count = 0
        
        print("   测速进度: ", end="", flush=True)
        
        for i, (_, row) in enumerate(sources_df.iterrows()):
            if not self._is_running:
                break
                
            url = row['stream_url']
            result = results.get(url, SpeedTestResult(url=url, accessible=False))
            
            speed_results.append({
                'program_name': row['program_name'],
                'stream_url': url,
                'accessible': result.accessible,
                'speed': result.speed,
                'original_name': row.get('original_name', ''),
                'stream_type': result.stream_type.value,
                'content_type': result.content_type,
                'file_size': result.file_size
            })
            
            if result.accessible:
                accessible_count += 1
                # 智能速度等级显示
                if result.speed < 1.5: 
                    print("🚀", end="", flush=True)  # 极快
                elif result.speed < 3: 
                    print("⚡", end="", flush=True)  # 快速
                elif result.speed < 6: 
                    print("✅", end="", flush=True)  # 可用
                elif result.speed < 10: 
                    print("🐢", end="", flush=True)  # 慢速
                else: 
                    print("🔴", end="", flush=True)  # 超慢
            else:
                print("❌", end="", flush=True)  # 不可用
            
            # 实时进度更新
            if (i + 1) % 10 == 0 or (i + 1) == len(sources_df):
                self._print_progress_bar(i + 1, len(sources_df), "   测速进度", f"{accessible_count}可用")
        
        print()
        
        try:
            result_df = pd.DataFrame(speed_results)
            
            # 智能过滤
            initial_count = len(result_df)
            accessible_df = result_df[result_df['accessible']].copy()
            
            if not accessible_df.empty:
                # 应用智能速率过滤
                if self.config.open_filter_speed:
                    speed_filtered = accessible_df[
                        (accessible_df['speed'] >= self.config.min_speed) & 
                        (accessible_df['speed'] <= self.config.max_speed)
                    ]
                    speed_filtered_count = len(accessible_df) - len(speed_filtered)
                    self.stats.speed_filtered = speed_filtered_count
                    accessible_df = speed_filtered
                
                # 应用质量控制过滤
                if self.config.enable_quality_control:
                    quality_filtered = accessible_df[
                        (accessible_df['file_size'] >= self.config.min_content_length) & 
                        (accessible_df['file_size'] <= self.config.max_content_length)
                    ]
                    quality_filtered_count = len(accessible_df) - len(quality_filtered)
                    self.stats.quality_filtered = quality_filtered_count
                    accessible_df = quality_filtered
            
            self.stats.sources_tested = len(sources_df)
            self.stats.sources_available = len(accessible_df)
            
            # 计算统计信息
            avg_speed = accessible_df['speed'].mean() if not accessible_df.empty else 0
            total_filtered = initial_count - len(accessible_df)
            
            self.progress.update_substep(
                f"测速完成: {len(accessible_df)}/{len(sources_df)} 可用 "
                f"(平均{avg_speed:.2f}秒, 过滤{total_filtered}个)", 
                "✅"
            )
            
            return accessible_df
            
        except Exception as e:
            logging.error(f"❌ 处理测速结果时出错: {e}")
            self.stats.errors_encountered += 1
            return pd.DataFrame()

    def load_template(self) -> Optional[Dict[str, List[str]]]:
        """加载频道模板文件"""
        template_file = Path(self.config.template_file)
        
        if not template_file.exists():
            logging.error(f"❌ 模板文件 {template_file} 不存在")
            return None
            
        self.progress.update_substep("加载模板文件...", "📋")
        categories: Dict[str, List[str]] = {}
        current_category: Optional[str] = None
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if not self._is_running:
                        break
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                        
                    category_match = self._patterns['category'].match(line)
                    if category_match:
                        current_category = category_match.group(1).strip()
                        if current_category:
                            categories[current_category] = []
                    elif current_category and line:
                        channel_name = line.split(',')[0].strip() if ',' in line else line.strip()
                        if channel_name:
                            categories[current_category].append(channel_name)
        
        except Exception as e:
            logging.error(f"❌ 读取模板文件失败: {e}")
            return None
        
        if not categories:
            logging.error("❌ 模板文件中未找到有效的频道分类")
            return None
            
        total_channels = sum(len(channels) for channels in categories.values())
        self.stats.categories_processed = len(categories)
        self.progress.update_substep(f"加载完成: {len(categories)} 个分类, {total_channels} 个频道", "✅")
        
        return categories

    def clean_channel_name(self, name: str) -> str:
        """频道名称清理 - 智能版本"""
        if not name:
            return ""
        try:
            cleaned = name.lower().strip()
            
            # 移除质量后缀但保留更多原始信息
            cleaned = self._patterns['quality_suffix'].sub(' ', cleaned)
            cleaned = self._patterns['brackets'].sub('', cleaned)
            
            # 标准化频道代码
            code_match = self._patterns['channel_code'].search(cleaned)
            if code_match:
                prefix, number = code_match.group(1).upper(), code_match.group(2)
                cleaned = f"{prefix} {number}"
            
            # 智能特殊字符处理
            cleaned = self._patterns['special_chars'].sub(' ', cleaned)
            cleaned = self._patterns['whitespace'].sub(' ', cleaned).strip()
            
            return cleaned
        except Exception:
            return name.lower() if name else ""

    def similarity_score(self, str1: str, str2: str) -> int:
        """计算两个字符串的相似度分数（0-100） - 智能优化版本"""
        if not str1 or not str2:
            return 0
        try:
            clean_str1, clean_str2 = self.clean_channel_name(str1), self.clean_channel_name(str2)
            if not clean_str1 or not clean_str2:
                return 0
            
            # 完全匹配
            if clean_str1 == clean_str2:
                return 100
            
            # 包含关系
            if clean_str1 in clean_str2:
                return 90
            if clean_str2 in clean_str1:
                return 85
            
            # 编辑距离相似度
            def edit_distance_similarity(s1: str, s2: str) -> float:
                if len(s1) > len(s2):
                    s1, s2 = s2, s1
                if not s2:
                    return 0.0
                distances = range(len(s1) + 1)
                for i2, c2 in enumerate(s2):
                    distances_ = [i2 + 1]
                    for i1, c1 in enumerate(s1):
                        if c1 == c2:
                            distances_.append(distances[i1])
                        else:
                            distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
                    distances = distances_
                max_len = max(len(s1), len(s2))
                return (1 - distances[-1] / max_len) * 100 if max_len > 0 else 0
            
            edit_score = edit_distance_similarity(clean_str1, clean_str2)
            
            # Jaccard相似度
            set1, set2 = set(clean_str1), set(clean_str2)
            intersection, union = len(set1 & set2), len(set1 | set2)
            jaccard_similarity = (intersection / union) * 100 if union > 0 else 0
            
            # 智能组合分数
            if len(clean_str1) > 3 and len(clean_str2) > 3:
                # 长字符串更依赖编辑距离
                final_score = (edit_score * 0.7 + jaccard_similarity * 0.3)
            else:
                # 短字符串更依赖Jaccard
                final_score = (edit_score * 0.4 + jaccard_similarity * 0.6)
            
            return max(0, min(100, int(final_score)))
        except Exception:
            return 0

    def match_with_template(self, speed_tested_df: pd.DataFrame, template_categories: Dict[str, List[str]]) -> Dict[str, Any]:
        """模板匹配和排序 - 第三步：智能匹配"""
        self.progress.update_substep("启动智能频道匹配...", "🎯")
        
        if speed_tested_df.empty or not template_categories:
            logging.error("❌ 测速数据或模板分类为空")
            return {}
        
        final_data = {}
        total_sources, channels_with_sources = 0, 0
        
        print("   匹配进度: ", end="", flush=True)
        
        total_channels = sum(len(channels) for channels in template_categories.values())
        processed_channels = 0
        
        # 为每个分类和频道进行智能匹配
        for category, channels in template_categories.items():
            if not self._is_running:
                break
            final_data[category] = {}
            
            for channel in channels:
                if not self._is_running:
                    break
                    
                processed_channels += 1
                best_sources = []
                best_score = 0
                
                # 为每个频道寻找最佳匹配的源
                for _, source_row in speed_tested_df.iterrows():
                    score = self.similarity_score(channel, source_row['program_name'])
                    if score > best_score and score >= self.config.similarity_threshold:
                        best_score = score
                
                # 获取所有达到最佳分数的源
                if best_score > 0:
                    matching_sources = []
                    for _, source_row in speed_tested_df.iterrows():
                        score = self.similarity_score(channel, source_row['program_name'])
                        if score == best_score:
                            matching_sources.append({
                                'stream_url': source_row['stream_url'],
                                'speed': source_row['speed'],
                                'match_score': score,
                                'original_name': source_row['program_name']
                            })
                    
                    # 智能排序并选择前N个
                    matching_sources.sort(key=lambda x: x['speed'])
                    best_sources = matching_sources[:self.config.max_sources_per_channel]
                
                if best_sources:
                    final_data[category][channel] = best_sources
                    source_count = len(best_sources)
                    total_sources += source_count
                    channels_with_sources += 1
                    
                    # 智能匹配质量显示
                    if best_score >= 90:
                        if source_count >= 5: print("🎯", end="", flush=True)
                        elif source_count >= 3: print("⭐", end="", flush=True)
                        else: print("✅", end="", flush=True)
                    elif best_score >= 70:
                        if source_count >= 3: print("🔶", end="", flush=True)
                        else: print("👍", end="", flush=True)
                    elif best_score >= 50:
                        print("🔹", end="", flush=True)
                    else:
                        print("▪️", end="", flush=True)
                else:
                    final_data[category][channel] = []
                    print("❌", end="", flush=True)
                
                # 实时进度更新
                if processed_channels % 10 == 0 or processed_channels == total_channels:
                    self._print_progress_bar(processed_channels, total_channels, "   匹配进度", f"{channels_with_sources}有源")
        
        print()
        
        self.stats.channels_matched = channels_with_sources
        self.stats.total_sources_found = total_sources
        
        coverage_rate = (channels_with_sources / total_channels * 100) if total_channels > 0 else 0
        avg_sources_per_channel = total_sources / channels_with_sources if channels_with_sources > 0 else 0
        
        self.progress.update_substep(
            f"匹配完成: {channels_with_sources}/{total_channels} 频道有源 "
            f"({coverage_rate:.1f}%覆盖率, 平均{avg_sources_per_channel:.1f}源/频道)", 
            "✅"
        )
        
        return final_data

    def save_output_files(self, final_data: Dict[str, Any]) -> bool:
        """保存输出文件 - 第四步：生成"""
        self.progress.update_substep("保存输出文件...", "💾")
        
        if not final_data:
            logging.error("❌ 没有数据需要保存")
            return False
        
        success_count = 0
        
        # 保存TXT格式
        try:
            with open(self.config.output_txt, 'w', encoding='utf-8') as f:
                f.write("# IPTV播放列表 - 生成时间: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
                f.write("# 流程: 智能抓取 → 精准测速 → 模板匹配 → 生成文件\n")
                f.write("# 每个频道提供多个备用源，按速度排序\n# 格式: 频道名称,直播流地址\n\n")
                
                for category, channels in final_data.items():
                    f.write(f"{category},#genre#\n")
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f"{channel},{source['stream_url']}\n")
                    f.write("\n")
            
            success_count += 1
            file_size = os.path.getsize(self.config.output_txt)
            self.progress.update_substep(f"TXT文件已保存 ({file_size} 字节)", "✅")
        except Exception as e:
            logging.error(f"❌ 保存TXT文件失败: {e}")
            self.stats.errors_encountered += 1
        
        # 保存M3U格式
        try:
            with open(self.config.output_m3u, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n#PLAYLIST: IPTV智能列表\n")
                f.write("#GENERATED: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
                f.write("#PROCESS: 智能抓取→精准测速→模板匹配→生成文件\n")
                
                for category, channels in final_data.items():
                    for channel, sources in channels.items():
                        for idx, source in enumerate(sources, 1):
                            # 智能质量标识
                            quality_info = ""
                            if source['speed'] < 1.5:
                                quality_info = " [极速]"
                            elif source['speed'] < 3:
                                quality_info = " [快速]"
                            elif source['speed'] < 6:
                                quality_info = " [稳定]"
                            elif source['speed'] < 10:
                                quality_info = " [慢速]"
                            
                            display_name = f"{channel}{quality_info}" if len(sources) == 1 else f"{channel} [源{idx}]{quality_info}"
                            f.write(f'#EXTINF:-1 tvg-name="{channel}" group-title="{category}",{display_name}\n')
                            f.write(f"{source['stream_url']}\n")
            
            success_count += 1
            file_size = os.path.getsize(self.config.output_m3u)
            self.progress.update_substep(f"M3U文件已保存 ({file_size} 字节)", "✅")
        except Exception as e:
            logging.error(f"❌ 保存M3U文件失败: {e}")
            self.stats.errors_encountered += 1
            
        return success_count == 2

    def print_detailed_statistics(self, final_data: Dict[str, Any]) -> None:
        """打印详细统计信息"""
        if not self.config.show_detailed_stats:
            return
            
        print("\n" + "="*60)
        print("📈 详细统计报告")
        print("="*60)
        
        if not final_data:
            print("❌ 没有数据可统计")
            return
        
        total_channels, total_sources = 0, 0
        category_details = []
        
        # 统计每个分类的情况
        for category, channels in final_data.items():
            category_channels, category_sources = 0, 0
            for channel, sources in channels.items():
                if sources:
                    category_channels += 1
                    category_sources += len(sources)
            
            if category_channels > 0:
                category_details.append((category, category_channels, category_sources))
                total_channels += category_channels
                total_sources += category_sources
        
        # 按频道数量排序
        category_details.sort(key=lambda x: x[1], reverse=True)
        
        print("📊 分类统计:")
        for category, channel_count, source_count in category_details:
            avg_sources = source_count / channel_count if channel_count > 0 else 0
            coverage = channel_count / len(final_data[category]) * 100 if final_data[category] else 0
            print(f"  📺 {category:<12}: {channel_count:2d}频道 ({coverage:5.1f}%) | {source_count:3d}源 (平均{avg_sources:.1f}源/频道)")
        
        print("-"*60)
        total_template_channels = sum(len(channels) for channels in final_data.values())
        coverage_rate = (self.stats.channels_with_sources / total_template_channels * 100) if total_template_channels > 0 else 0
        print(f"📈 总体统计:")
        print(f"  🎯 频道覆盖率: {self.stats.channels_with_sources}/{total_template_channels} ({coverage_rate:.1f}%)")
        print(f"  🔗 总源数量: {total_sources} (平均{total_sources/total_channels:.1f}源/频道)" if total_channels > 0 else "  🔗 总源数量: 0")
        print(f"  📁 分类数量: {self.stats.categories_processed}")
        
        print("-"*60)
        print(f"⚙️  处理统计:")
        print(f"  🌐 源抓取: {self.stats.sources_fetched}成功")
        print(f"  🔧 流解析: {self.stats.streams_parsed}个流")
        print(f"  🎯 频道匹配: {self.stats.channels_matched}个频道")
        print(f"  ⚡ 源测速: {self.stats.sources_tested}测试, {self.stats.sources_available}可用")
        if self.stats.quality_filtered > 0:
            print(f"  🎯 质量过滤: {self.stats.quality_filtered}个")
        if self.stats.speed_filtered > 0:
            print(f"  🐢 速度过滤: {self.stats.speed_filtered}个")
        if self.stats.errors_encountered > 0:
            print(f"  ⚠️  遇到错误: {self.stats.errors_encountered}个")

    def _backup_existing_files(self) -> None:
        """备份现有文件"""
        backup_dir = Path(self.config.backup_dir)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        for file_name in [self.config.output_txt, self.config.output_m3u]:
            file_path = Path(file_name)
            if file_path.exists():
                backup_path = backup_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}"
                try:
                    shutil.copy2(file_path, backup_path)
                    logging.info(f"📦 已备份: {file_name}")
                except Exception as e:
                    logging.warning(f"⚠️ 备份文件 {file_name} 失败: {e}")

    def create_demo_template(self) -> bool:
        """创建示例模板文件"""
        demo_content = """# IPTV频道模板文件
# 格式: 分类名称,#genre#
#       频道名称1
#       频道名称2

央视频道,#genre#
CCTV-1
CCTV-2
CCTV-3
CCTV-4
CCTV-5
CCTV-5+
CCTV-6
CCTV-7
CCTV-8
CCTV-9
CCTV-10
CCTV-11
CCTV-12
CCTV-13
CCTV-14
CCTV-15

卫视频道,#genre#
湖南卫视
浙江卫视
江苏卫视
东方卫视
北京卫视
天津卫视
山东卫视
广东卫视
深圳卫视
安徽卫视

地方频道,#genre#
北京科教
北京纪实
北京生活
北京财经
北京文艺

高清频道,#genre#
CCTV-1高清
CCTV-5高清
湖南卫视高清
浙江卫视高清
江苏卫视高清
"""
        try:
            with open(self.config.template_file, 'w', encoding='utf-8') as f:
                f.write(demo_content)
            logging.info(f"✅ 已创建示例模板文件: {self.config.template_file}")
            return True
        except Exception as e:
            logging.error(f"❌ 创建模板文件失败: {e}")
            self.stats.errors_encountered += 1
            return False

    def cleanup(self) -> None:
        """清理资源"""
        try:
            if hasattr(self, 'speed_engine'):
                self.speed_engine.stop()
            temp_dir = Path(self.config.temp_dir)
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            logging.info("✅ 资源清理完成")
        except Exception as e:
            logging.debug(f"清理资源时出错: {e}")

    def run(self) -> None:
        """主运行函数 - 按照优化后的流程执行"""
        print("=" * 60)
        print("🎬 IPTV智能管理工具 - GitHub Actions优化版 v6.1")
        print("🔧 流程: 智能抓取 → 精准测速 → 模板匹配 → 生成文件")
        print("=" * 60)
        
        start_time = time.time()
        
        try:
            # 定义优化后的处理步骤
            step_names = [
                "环境准备和备份",
                "智能多源抓取", 
                "解析原始数据",
                "精准测速过滤",
                "加载频道模板", 
                "智能匹配排序",
                "生成播放列表",
                "保存输出文件"
            ]
            self.progress.start_progress(step_names)
            
            # 步骤1: 环境准备
            self.progress.next_step("初始化环境和备份文件")
            self._backup_existing_files()
            
            # 检查模板文件
            template_path = Path(self.config.template_file)
            if not template_path.exists():
                print("📝 未找到模板文件，创建示例模板...")
                if self.create_demo_template():
                    print(f"\n💡 模板文件已创建: {template_path.absolute()}")
                    print("💡 请编辑模板文件后重新运行程序")
                    return
            
            # 步骤2: 智能多源抓取
            self.progress.next_step("从多个源抓取流数据")
            content = self.fetch_all_streams()
            if not content:
                logging.error("❌ 抓取阶段失败，无法继续")
                return
            
            # 步骤3: 解析原始数据
            self.progress.next_step("解析和清理原始流数据")
            sources_df = self.organize_streams(content)
            if sources_df.empty:
                logging.error("❌ 解析阶段失败，无法继续")
                return
            
            # 步骤4: 精准测速和过滤
            self.progress.next_step("精准测速和智能筛选")
            speed_tested_df = self.speed_test_and_filter(sources_df)
            if speed_tested_df.empty:
                logging.error("❌ 测速阶段失败，没有可用的源")
                return
            
            # 步骤5: 加载频道模板
            self.progress.next_step("加载频道模板配置")
            template_categories = self.load_template()
            if not template_categories:
                logging.error("❌ 模板加载失败，无法继续")
                return
            
            # 步骤6: 智能匹配排序
            self.progress.next_step("智能匹配频道和排序")
            final_data = self.match_with_template(speed_tested_df, template_categories)
            if not final_data:
                logging.error("❌ 匹配阶段失败，没有生成有效数据")
                return
            
            # 步骤7: 生成播放列表
            self.progress.next_step("生成最终播放列表")
            # 数据已经在匹配阶段生成，这里主要是准备保存
            
            # 步骤8: 保存输出文件
            self.progress.next_step("保存TXT和M3U格式文件")
            if not self.save_output_files(final_data):
                logging.error("❌ 文件保存失败")
                return
            
            # 打印详细统计
            self.print_detailed_statistics(final_data)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            print("\n🎉 处理完成!")
            print(f"⏰ 总耗时: {elapsed_time:.2f} 秒")
            print(f"📁 生成文件:")
            print(f"   📄 {Path(self.config.output_txt).absolute()}")
            print(f"   📄 {Path(self.config.output_m3u).absolute()}")
            print(f"📊 最终结果: {self.stats.channels_with_sources}个频道有可用源")
                
        except KeyboardInterrupt:
            print("\n⚠️  用户中断操作")
            self.stats.errors_encountered += 1
        except Exception as e:
            print(f"\n❌ 程序运行出错: {e}")
            self.stats.errors_encountered += 1
            logging.exception("程序运行异常")
        finally:
            self.cleanup()
            
            if self.stats.errors_encountered > 0:
                logging.warning(f"⚠️ 本次运行遇到 {self.stats.errors_encountered} 个错误")


def main():
    """主函数 - GitHub Actions 优化版"""
    # 简化的日志配置，适合GitHub Actions
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    try:
        manager = IPTVManager()
        manager.run()
    except Exception as e:
        logging.error(f"程序运行失败: {e}")
        # 在GitHub Actions中，非零退出码会标记工作流为失败
        sys.exit(1)


if __name__ == "__main__":
    main()
