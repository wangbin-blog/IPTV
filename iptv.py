#!/usr/bin/env python3
"""
🎬 IPTV智能管理工具 - 流程优化版 v5.1
流程：智能抓取 → 测速过滤 → 模板匹配 → 生成文件
特点：优化处理流程 + 提升匹配精度 + 增强稳定性
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


@dataclass
class SpeedTestResult:
    """测速结果数据类"""
    url: str
    accessible: bool = False
    speed: float = float('inf')
    stream_type: StreamType = StreamType.UNKNOWN
    error_message: str = ""


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


# ==================== 配置管理系统 ====================

class ConfigManager:
    """配置管理系统"""
    
    def __init__(self):
        """初始化配置管理器"""
        # 文件配置
        self.template_file: str = "demo.txt"
        self.output_txt: str = "iptv.txt"
        self.output_m3u: str = "iptv.m3u"
        self.temp_dir: str = "temp"
        self.cache_dir: str = "cache"
        self.backup_dir: str = "backup"
        
        # 网络配置
        self.request_timeout: int = 20
        self.request_retries: int = 3
        self.max_workers: int = 8  # 增加并发数以加快抓取速度
        self.connection_pool_size: int = 15
        
        # 测速配置
        self.open_speed_test: bool = True
        self.speed_test_limit: int = 6  # 测速并发数
        self.speed_test_timeout: int = 10  # 增加测速超时
        self.ffmpeg_test_duration: int = 5
        self.ffmpeg_process_timeout: int = 15
        self.min_test_interval: int = 300
        
        # 过滤配置
        self.open_filter_speed: bool = True
        self.min_speed: float = 0.5  # 降低最小速度要求，保留更多源
        self.open_filter_resolution: bool = False
        self.min_resolution: int = 720
        self.max_resolution: int = 2160
        self.speed_test_filter_host: bool = True
        
        # 匹配配置
        self.similarity_threshold: int = 50  # 降低阈值以匹配更多频道
        self.max_sources_per_channel: int = 10  # 增加每个频道的最大源数
        self.min_similarity_high: int = 80
        self.min_similarity_medium: int = 60
        
        # 质量控制
        self.min_stream_size: int = 512  # 降低最小流大小要求
        self.max_url_length: int = 500
        self.max_content_length: int = 52428800
        
        # 显示配置
        self.progress_bar_width: int = 50
        self.show_detailed_stats: bool = True
        
        # 源URL配置 - 增加更多源以提高覆盖率
        self.source_urls: List[str] = [
            "https://live.zbds.top/tv/iptv6.txt",
            "https://live.zbds.top/tv/iptv4.txt",
            "http://home.jundie.top:81/top/tvbox.txt",
            "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
            "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
            "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/global.m3u",
            "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
            "https://raw.githubusercontent.com/iptv-org/iptv/gh-pages/countries/cn.m3u",
            "https://ghfast.top/raw.githubusercontent.com/Supprise0901/TVBox_live/main/live.txt",
            "https://raw.githubusercontent.com/Guovin/iptv-database/master/result.txt",  
            "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/cn.m3u",
            "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
            "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
            "http://47.120.41.246:8899/zb.txt",
        ]
        
        # HTTP请求头配置
        self.headers: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        }


# ==================== 进度显示管理器 ====================

class ProgressDisplay:
    """进度显示管理器"""
    
    def __init__(self):
        self.start_time: Optional[float] = None
        self.current_step: int = 0
        self.total_steps: int = 0
        self.step_names: List[str] = []
    
    def start_progress(self, step_names: List[str]) -> None:
        """开始进度跟踪"""
        self.step_names = step_names
        self.total_steps = len(step_names)
        self.current_step = 0
        self.start_time = time.time()
        self._print_header()
    
    def next_step(self, message: str = "") -> None:
        """进入下一步"""
        self.current_step += 1
        if self.current_step <= self.total_steps:
            step_name = self.step_names[self.current_step - 1]
            self._print_step(step_name, message)
    
    def update_substep(self, message: str, symbol: str = "🔹") -> None:
        """更新子步骤进度"""
        elapsed = time.time() - (self.start_time or time.time())
        print(f"  {symbol} [{elapsed:6.1f}s] {message}")
    
    def _print_header(self) -> None:
        """打印进度头"""
        print("\n" + "="*70)
        print("🎬 IPTV智能管理工具 - 流程优化版 v5.1")
        print("="*70)
    
    def _print_step(self, step_name: str, message: str) -> None:
        """打印步骤信息"""
        elapsed = time.time() - (self.start_time or time.time())
        print(f"\n📋 步骤 {self.current_step}/{self.total_steps}: {step_name}")
        if message:
            print(f"   📝 {message}")
        print(f"   ⏰ 已用时: {elapsed:.1f}秒")


# ==================== 测速引擎 ====================

class SpeedTestEngine:
    """测速引擎核心类"""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.session = self._create_session()
        self.ffmpeg_available = self._check_ffmpeg()
        self._stop_event = threading.Event()
        self._patterns = self._compile_patterns()
    
    def _create_session(self) -> requests.Session:
        """创建HTTP会话"""
        session = requests.Session()
        session.headers.update(self.config.headers)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.config.connection_pool_size,
            pool_maxsize=self.config.connection_pool_size,
            max_retries=3  # 增加重试次数
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
    
    def _compile_patterns(self) -> Dict[str, re.Pattern]:
        """编译正则表达式模式"""
        return {
            'stream_protocol': re.compile(r'^(https?|rtmp|rtsp)://', re.IGNORECASE)
        }
    
    def _check_ffmpeg(self) -> bool:
        """检查FFmpeg是否可用"""
        try:
            result = subprocess.run(
                ['ffmpeg', '-version'], 
                capture_output=True, 
                timeout=5, 
                check=False
            )
            available = result.returncode == 0
            if available:
                logging.info("✅ FFmpeg可用，将使用FFmpeg进行精确测速")
            else:
                logging.info("⚠️ FFmpeg不可用，将使用HTTP测速")
            return available
        except Exception:
            logging.info("⚠️ FFmpeg检查失败，将使用HTTP测速")
            return False
    
    def stop(self) -> None:
        """停止测速"""
        self._stop_event.set()
    
    def _detect_stream_type(self, url: str) -> StreamType:
        """检测流媒体类型"""
        if not url:
            return StreamType.UNKNOWN
        
        url_lower = url.lower()
        if '.m3u8' in url_lower:
            return StreamType.M3U8
        elif '.ts' in url_lower:
            return StreamType.TS
        elif '.flv' in url_lower:
            return StreamType.FLV
        elif '.mp4' in url_lower:
            return StreamType.MP4
        elif url_lower.startswith('rtmp://'):
            return StreamType.RTMP
        elif url_lower.startswith('rtsp://'):
            return StreamType.RTSP
        else:
            return StreamType.UNKNOWN
    
    def speed_test_ffmpeg(self, url: str) -> Tuple[bool, float]:
        """使用FFmpeg进行流媒体测速"""
        if not self.ffmpeg_available:
            return False, float('inf')
        
        temp_file = Path(self.config.temp_dir) / f'test_{hash(url) & 0xFFFFFFFF}.ts'
        
        try:
            cmd = [
                'ffmpeg', '-y', 
                '-timeout', '10000000',  # 增加超时时间
                '-rw_timeout', '10000000',
                '-i', url,
                '-t', str(self.config.ffmpeg_test_duration),
                '-c', 'copy',
                '-f', 'mpegts',
                '-max_muxing_queue_size', '1024',
                str(temp_file)
            ]
            
            start_time = time.time()
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE
            )
            
            try:
                stdout, stderr = process.communicate(
                    timeout=self.config.ffmpeg_process_timeout
                )
                end_time = time.time()
                
                if (process.returncode == 0 and 
                    temp_file.exists() and 
                    temp_file.stat().st_size > self.config.min_stream_size):
                    return True, end_time - start_time
                
                return False, float('inf')
                
            except subprocess.TimeoutExpired:
                process.kill()
                return False, float('inf')
                
        except Exception:
            return False, float('inf')
        finally:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
    
    def speed_test_http(self, url: str) -> Tuple[bool, float]:
        """HTTP测速"""
        try:
            start_time = time.time()
            # 使用GET请求而不是HEAD，因为有些服务器HEAD请求可能不准确
            response = self.session.get(
                url, 
                timeout=self.config.speed_test_timeout,
                allow_redirects=True,
                stream=True
            )
            end_time = time.time()
            
            if response.status_code in [200, 206, 302, 301, 307]:
                # 立即关闭连接，我们只需要确认可访问性
                response.close()
                content_type = response.headers.get('Content-Type', '').lower()
                # 放宽内容类型检查
                if any(ct in content_type for ct in ['video/', 'audio/', 'application/', 'text/']):
                    return True, end_time - start_time
            
            return False, float('inf')
            
        except Exception:
            return False, float('inf')
    
    def test_single_url(self, url: str) -> SpeedTestResult:
        """测试单个URL"""
        if self._stop_event.is_set():
            return SpeedTestResult(url=url, accessible=False)
        
        result = SpeedTestResult(url=url)
        result.stream_type = self._detect_stream_type(url)
        
        try:
            # 优先使用FFmpeg进行精确测速
            if result.stream_type in [StreamType.M3U8, StreamType.TS, StreamType.FLV]:
                if self.ffmpeg_available:
                    result.accessible, result.speed = self.speed_test_ffmpeg(url)
                else:
                    result.accessible, result.speed = self.speed_test_http(url)
            else:
                result.accessible, result.speed = self.speed_test_http(url)
                
        except Exception as e:
            result.accessible = False
            result.error_message = str(e)
        
        return result
    
    def batch_speed_test(self, urls: List[str], 
                        progress_callback: Callable = None) -> Dict[str, SpeedTestResult]:
        """批量测速 - 优化版本"""
        if not self.config.open_speed_test:
            # 如果测速关闭，返回所有URL为可访问
            return {url: SpeedTestResult(url=url, accessible=True) for url in urls}
        
        self._stop_event.clear()
        results = {}
        
        def test_with_callback(url: str) -> Tuple[str, SpeedTestResult]:
            if self._stop_event.is_set():
                return url, SpeedTestResult(url=url, accessible=False)
            
            result = self.test_single_url(url)
            if progress_callback:
                progress_callback(url, result)
            return url, result
        
        try:
            # 使用更智能的线程池管理
            with ThreadPoolExecutor(max_workers=self.config.speed_test_limit) as executor:
                # 分批提交任务，避免内存占用过高
                batch_size = 50
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
                            url, result = future.result(timeout=self.config.speed_test_timeout + 15)
                            results[url] = result
                        except Exception as e:
                            url = future_to_url[future]
                            results[url] = SpeedTestResult(
                                url=url, 
                                accessible=False, 
                                error_message=str(e)
                            )
                        
        except Exception as e:
            logging.error(f"批量测速失败: {e}")
        
        return results


# ==================== IPTV核心管理器 ====================

class IPTVManager:
    """IPTV智能管理工具核心类"""
    
    def __init__(self, config: ConfigManager = None) -> None:
        self.config: ConfigManager = config or ConfigManager()
        self.stats: ProcessingStats = ProcessingStats()
        self.progress: ProgressDisplay = ProgressDisplay()
        self.speed_engine: SpeedTestEngine = SpeedTestEngine(self.config)
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
            'special_chars': re.compile(r'[^\w\u4e00-\u9fa5\s-]')
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
        """验证URL格式和安全性"""
        if not url or not isinstance(url, str) or len(url) > self.config.max_url_length:
            return False
        try:
            result = urlparse(url)
            return (result.scheme in ['http', 'https', 'rtmp', 'rtsp'] and 
                    bool(result.netloc) and 
                    '//' not in result.path and '\\' not in result.path)
        except Exception:
            return False

    @contextmanager
    def _request_context(self, url: str, timeout: int = None):
        """请求上下文管理器"""
        timeout = timeout or self.config.request_timeout
        start_time = time.time()
        response = None
        try:
            response = self.speed_engine.session.get(url, timeout=timeout, stream=True, allow_redirects=True)
            yield response
        finally:
            if response:
                response.close()
            elapsed = time.time() - start_time
            logging.debug(f"请求 {url} 耗时: {elapsed:.2f}秒")

    def fetch_streams_from_url(self, url: str) -> Optional[str]:
        """从URL获取流数据 - 优化版本"""
        if not self.validate_url(url):
            logging.error(f"❌ 无效的URL: {url}")
            return None
            
        for attempt in range(self.config.request_retries):
            if not self._is_running:
                return None
            try:
                # 递增超时时间
                timeout = self.config.request_timeout + (attempt * 8)
                with self._request_context(url, timeout) as response:
                    if response.status_code == 200:
                        # 使用流式读取，避免大文件内存问题
                        content_chunks = []
                        total_size = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            if not self._is_running:
                                return None
                            content_chunks.append(chunk)
                            total_size += len(chunk)
                            # 如果内容太大，提前终止
                            if total_size > self.config.max_content_length:
                                logging.warning(f"内容过大，跳过: {url}")
                                return None
                        
                        content = b''.join(content_chunks).decode('utf-8', errors='ignore')
                        if len(content) >= self.config.min_stream_size:
                            self.stats.sources_fetched += 1
                            return content
                    elif response.status_code == 429:  # 频率限制
                        wait_time = (attempt + 1) * 15
                        logging.info(f"⚠️ 频率限制，等待 {wait_time} 秒后重试: {url}")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code >= 500:  # 服务器错误
                        logging.warning(f"⚠️ 服务器错误 {response.status_code}，重试: {url}")
                        time.sleep((attempt + 1) * 5)
                        continue
            except Exception as e:
                logging.debug(f"请求失败 (尝试 {attempt + 1}): {url} - {e}")
                if attempt < self.config.request_retries - 1:
                    time.sleep((attempt + 1) * 3)
        return None

    def fetch_all_streams(self) -> str:
        """获取所有源的流数据 - 优化版本"""
        self.progress.update_substep("开始多源抓取...", "🌐")
        
        if not self.config.source_urls:
            logging.error("❌ 没有配置源URL")
            return ""
        
        all_streams: List[str] = []
        successful_sources = 0
        
        print("   抓取进度: ", end="", flush=True)
        
        try:
            # 使用更智能的线程池管理
            with ThreadPoolExecutor(max_workers=min(self.config.max_workers, len(self.config.source_urls))) as executor:
                future_to_url = {executor.submit(self.fetch_streams_from_url, url): url for url in self.config.source_urls}
                
                for i, future in enumerate(as_completed(future_to_url)):
                    if not self._is_running:
                        break
                    url = future_to_url[future]
                    try:
                        content = future.result(timeout=self.config.request_timeout + 15)
                        if content:
                            all_streams.append(content)
                            successful_sources += 1
                            print("✅", end="", flush=True)
                        else:
                            print("❌", end="", flush=True)
                    except Exception as e:
                        logging.debug(f"抓取失败: {url} - {e}")
                        print("💥", end="", flush=True)
                    
                    # 更频繁的进度更新
                    if (i + 1) % 5 == 0 or (i + 1) == len(self.config.source_urls):
                        self._print_progress_bar(i + 1, len(self.config.source_urls), "   抓取进度", f"{successful_sources}成功")
        
        except Exception as e:
            logging.error(f"❌ 并发获取失败: {e}")
            return ""
        
        print()
        total_content = "\n".join(all_streams)
        self.progress.update_substep(f"成功获取 {successful_sources}/{len(self.config.source_urls)} 个源, 总数据量: {len(total_content)} 字符", "✅")
        return total_content

    def _extract_program_name(self, extinf_line: str) -> str:
        """从EXTINF行提取节目名称"""
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
                # 清理名称但保留更多信息
                name = self._patterns['brackets'].sub('', name)
                name = self._patterns['quality_suffix'].sub('', name)
                return name.strip() if name and name != "未知频道" else "未知频道"
        except Exception:
            pass
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
                # 尝试多种分隔符
                separators = [',', ' ', '\t', '|', '$']
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
            except Exception:
                continue
        return streams

    def organize_streams(self, content: str) -> pd.DataFrame:
        """整理流数据 - 第一步：抓取和解析"""
        self.progress.update_substep("解析流数据...", "🔍")
        
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
            
            # 数据清理
            initial_count = len(df)
            df = df.dropna()
            df = df[df['program_name'].str.len() > 0]
            df = df[df['stream_url'].str.len() > 0]
            df['url_valid'] = df['stream_url'].apply(self.validate_url)
            df = df[df['url_valid']].drop('url_valid', axis=1)
            df = df.drop_duplicates(subset=['program_name', 'stream_url'], keep='first')
            
            final_count = len(df)
            self.progress.update_substep(f"解析完成: {initial_count} → {final_count} 个流 (移除 {initial_count - final_count} 个无效数据)", "✅")
            
            return df
            
        except Exception as e:
            logging.error(f"❌ 数据处理错误: {e}")
            self.stats.errors_encountered += 1
            return pd.DataFrame()

    def speed_test_and_filter(self, sources_df: pd.DataFrame) -> pd.DataFrame:
        """测速和过滤 - 第二步：测速"""
        self.progress.update_substep("开始智能测速...", "⏱️")
        
        if sources_df.empty:
            logging.error("❌ 没有需要测速的源")
            return pd.DataFrame()
            
        urls = sources_df['stream_url'].tolist()
        
        # 进度回调函数
        def progress_callback(url: str, result: SpeedTestResult):
            pass  # 在批量测速中统一显示进度
        
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
                'stream_type': result.stream_type.value
            })
            
            if result.accessible:
                accessible_count += 1
                # 根据响应时间显示不同符号
                if result.speed < 2: 
                    print("🚀", end="", flush=True)  # 极快
                elif result.speed < 5: 
                    print("⚡", end="", flush=True)  # 快速
                elif result.speed < 10: 
                    print("✅", end="", flush=True)  # 可用
                else: 
                    print("🐢", end="", flush=True)  # 慢速
            else:
                print("❌", end="", flush=True)  # 不可用
            
            # 更频繁的进度更新
            if (i + 1) % 10 == 0 or (i + 1) == len(sources_df):
                self._print_progress_bar(i + 1, len(sources_df), "   测速进度", f"{accessible_count}可用")
        
        print()
        
        try:
            result_df = pd.DataFrame(speed_results)
            accessible_df = result_df[result_df['accessible']].copy()
            
            if not accessible_df.empty:
                # 应用速率过滤
                if self.config.open_filter_speed:
                    max_speed = 1.0 / self.config.min_speed if self.config.min_speed > 0 else float('inf')
                    accessible_df = accessible_df[accessible_df['speed'] <= max_speed]
                    filtered_count = len(result_df) - len(accessible_df)
                    if filtered_count > 0:
                        logging.info(f"📊 速率过滤移除 {filtered_count} 个慢速源")
            
            self.stats.sources_tested = len(sources_df)
            self.stats.sources_available = len(accessible_df)
            
            avg_speed = accessible_df['speed'].mean() if not accessible_df.empty else 0
            self.progress.update_substep(f"测速完成: {len(accessible_df)}/{len(sources_df)} 可用 (平均{avg_speed:.2f}秒)", "✅")
            
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
        """频道名称清理"""
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
            
            # 清理特殊字符
            cleaned = self._patterns['special_chars'].sub(' ', cleaned)
            cleaned = self._patterns['whitespace'].sub(' ', cleaned).strip()
            return cleaned
        except Exception:
            return name.lower() if name else ""

    def similarity_score(self, str1: str, str2: str) -> int:
        """计算两个字符串的相似度分数（0-100） - 优化版本"""
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
            
            # 组合分数（编辑距离权重0.6，Jaccard权重0.4）
            final_score = (edit_score * 0.6 + jaccard_similarity * 0.4)
            return max(0, min(100, int(final_score)))
        except Exception:
            return 0

    def match_with_template(self, speed_tested_df: pd.DataFrame, template_categories: Dict[str, List[str]]) -> Dict[str, Any]:
        """模板匹配和排序 - 第三步：匹配"""
        self.progress.update_substep("开始智能频道匹配...", "🎯")
        
        if speed_tested_df.empty or not template_categories:
            logging.error("❌ 测速数据或模板分类为空")
            return {}
        
        final_data = {}
        total_sources, channels_with_sources = 0, 0
        
        print("   匹配进度: ", end="", flush=True)
        
        total_channels = sum(len(channels) for channels in template_categories.values())
        processed_channels = 0
        
        # 为每个分类和频道进行匹配
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
                    
                    # 按速度排序并选择前N个
                    matching_sources.sort(key=lambda x: x['speed'])
                    best_sources = matching_sources[:self.config.max_sources_per_channel]
                
                if best_sources:
                    final_data[category][channel] = best_sources
                    source_count = len(best_sources)
                    total_sources += source_count
                    channels_with_sources += 1
                    
                    # 根据匹配质量和源数量显示符号
                    if best_score >= 90:
                        if source_count >= 5: print("🎯", end="", flush=True)
                        elif source_count >= 3: print("⭐", end="", flush=True)
                        else: print("✅", end="", flush=True)
                    elif best_score >= 70:
                        if source_count >= 3: print("🔶", end="", flush=True)
                        else: print("👍", end="", flush=True)
                    else:
                        print("🔹", end="", flush=True)
                else:
                    final_data[category][channel] = []
                    print("❌", end="", flush=True)
                
                # 进度更新
                if processed_channels % 10 == 0 or processed_channels == total_channels:
                    self._print_progress_bar(processed_channels, total_channels, "   匹配进度", f"{channels_with_sources}有源")
        
        print()
        
        self.stats.channels_matched = channels_with_sources
        self.stats.total_sources_found = total_sources
        
        coverage_rate = (channels_with_sources / total_channels * 100) if total_channels > 0 else 0
        self.progress.update_substep(f"匹配完成: {channels_with_sources}/{total_channels} 频道有源 ({coverage_rate:.1f}%覆盖率)", "✅")
        
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
                f.write("# 流程: 智能抓取 → 测速过滤 → 模板匹配 → 生成文件\n")
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
                f.write("#PROCESS: 智能抓取→测速过滤→模板匹配→生成文件\n")
                
                for category, channels in final_data.items():
                    for channel, sources in channels.items():
                        for idx, source in enumerate(sources, 1):
                            # 显示源的质量信息
                            quality_info = ""
                            if source['speed'] < 3:
                                quality_info = " [极速]"
                            elif source['speed'] < 6:
                                quality_info = " [快速]"
                            elif source['speed'] < 10:
                                quality_info = " [稳定]"
                            
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
            
        print("\n" + "="*70)
        print("📈 详细统计报告")
        print("="*70)
        
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
        
        print("-"*70)
        total_template_channels = sum(len(channels) for channels in final_data.values())
        coverage_rate = (self.stats.channels_with_sources / total_template_channels * 100) if total_template_channels > 0 else 0
        print(f"📈 总体统计:")
        print(f"  🎯 频道覆盖率: {self.stats.channels_with_sources}/{total_template_channels} ({coverage_rate:.1f}%)")
        print(f"  🔗 总源数量: {total_sources} (平均{total_sources/total_channels:.1f}源/频道)" if total_channels > 0 else "  🔗 总源数量: 0")
        print(f"  📁 分类数量: {self.stats.categories_processed}")
        
        print("-"*70)
        print(f"⚙️  处理统计:")
        print(f"  🌐 源抓取: {self.stats.sources_fetched}成功")
        print(f"  🔧 流解析: {self.stats.streams_parsed}个流")
        print(f"  🎯 频道匹配: {self.stats.channels_matched}个频道")
        print(f"  ⚡ 源测速: {self.stats.sources_tested}测试, {self.stats.sources_available}可用")
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
        print("=" * 70)
        print("🎬 IPTV智能管理工具 - 流程优化版 v5.1")
        print("🔧 流程: 智能抓取 → 测速过滤 → 模板匹配 → 生成文件")
        print("=" * 70)
        
        start_time = time.time()
        
        try:
            # 定义优化后的处理步骤
            step_names = [
                "环境准备和备份",
                "智能多源抓取", 
                "解析原始数据",
                "测速和过滤",
                "加载频道模板", 
                "模板匹配排序",
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
            
            # 步骤4: 测速和过滤
            self.progress.next_step("测速和筛选可用源")
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
            
            # 步骤6: 模板匹配排序
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
    """主函数"""
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('iptv_manager.log', encoding='utf-8', mode='w')
        ]
    )
    
    try:
        config = ConfigManager()
        manager = IPTVManager(config)
        manager.run()
    except Exception as e:
        logging.error(f"程序启动失败: {e}")
        print(f"\n❌ 程序启动失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
