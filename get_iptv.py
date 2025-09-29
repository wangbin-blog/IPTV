#!/usr/bin/env python3
"""
IPTV源处理工具 - 终极优化版
版本：5.0
功能：多源抓取、智能测速、严格过滤、模板匹配、性能监控、配置管理
"""

import os
import sys
import re
import time
import json
import random
import logging
import platform
import threading
import statistics
from typing import List, Dict, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse
import requests
import psutil
import yaml

# ======================== 初始化设置 =========================
class AppConfig:
    """应用全局配置"""
    NAME = "IPTV Processor"
    VERSION = "5.0"
    AUTHOR = "Optimized Edition"
    DEFAULT_TEMPLATE = "template.txt"
    CONFIG_FILES = ['config.yaml', 'config.yml', 'config.json']
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15)",
        "Mozilla/5.0 (X11; Linux x86_64)"
    ]

# ======================== 日志系统 =========================
def setup_logger():
    """配置日志系统"""
    logger = logging.getLogger('IPTV_Processor')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 文件处理器
    file_handler = logging.FileHandler('iptv_processor.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

# ======================== 核心数据模型 =========================
class ResolutionQuality(Enum):
    """分辨率质量等级"""
    UHD_8K = auto()
    UHD_4K = auto()
    FHD_1080P = auto()
    HD_720P = auto()
    SD_480P = auto()
    LOW_360P = auto()
    UNKNOWN = auto()

class ChannelStatus(Enum):
    """频道状态"""
    VALID = auto()
    INVALID = auto()
    TIMEOUT = auto()
    UNREACHABLE = auto()

@dataclass
class ChannelInfo:
    """频道信息"""
    name: str
    url: str
    delay: float = float('inf')
    speed: float = 0.0
    width: int = 0
    height: int = 0
    resolution: str = "unknown"
    quality: ResolutionQuality = ResolutionQuality.UNKNOWN
    status: ChannelStatus = ChannelStatus.INVALID
    last_checked: float = field(default_factory=time.time)
    source_hash: str = field(default="", repr=False)

    def __post_init__(self):
        """初始化后处理"""
        self.source_hash = hashlib.md5(f"{self.name}{self.url}".encode()).hexdigest()[:8]

@dataclass
class ProcessingStats:
    """处理统计信息"""
    total_sources: int = 0
    valid_sources: int = 0
    total_channels: int = 0
    speed_tested: int = 0
    template_matched: int = 0
    resolution_filtered: int = 0
    final_channels: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    
    @property
    def elapsed_time(self) -> float:
        return (self.end_time or time.time()) - self.start_time
    
    @property
    def success_rate(self) -> float:
        return (self.valid_sources / self.total_sources * 100) if self.total_sources > 0 else 0

# ======================== 工具类 =========================
class Console:
    """增强控制台输出"""
    
    COLORS = {
        'green': '\033[92m',
        'red': '\033[91m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'reset': '\033[0m'
    }
    
    ICONS = {
        'success': '✅',
        'error': '❌',
        'warning': '⚠️',
        'info': 'ℹ️'
    }
    
    _lock = threading.Lock()
    
    @classmethod
    def _init_colors(cls):
        """初始化颜色支持"""
        if platform.system() == "Windows":
            try:
                import colorama
                colorama.init()
            except ImportError:
                cls.COLORS = {k: '' for k in cls.COLORS}
    
    @classmethod
    def print(cls, message: str, color: str = None, icon: str = None, end: str = "\n"):
        """线程安全打印"""
        with cls._lock:
            color_code = cls.COLORS.get(color, '')
            icon_str = f"{cls.ICONS.get(icon, '')} " if icon else ""
            print(f"{color_code}{icon_str}{message}{cls.COLORS['reset']}", end=end)
    
    @classmethod
    def print_success(cls, message: str):
        cls.print(message, 'green', 'success')
        logger.info(f"SUCCESS: {message}")
    
    @classmethod
    def print_error(cls, message: str):
        cls.print(message, 'red', 'error')
        logger.error(f"ERROR: {message}")
    
    @classmethod
    def print_warning(cls, message: str):
        cls.print(message, 'yellow', 'warning')
        logger.warning(f"WARNING: {message}")
    
    @classmethod
    def print_info(cls, message: str):
        cls.print(message, 'blue', 'info')
        logger.info(f"INFO: {message}")
    
    @classmethod
    def print_separator(cls, title: str = "", length: int = 60):
        """打印分隔线"""
        with cls._lock:
            sep = "=" * length
            if title:
                print(f"\n{sep}\n{title.center(length)}\n{sep}")
            else:
                print(sep)

# 初始化控制台
Console._init_colors()

class TextUtils:
    """文本处理工具"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """清理文本"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """验证URL有效性"""
        return bool(url and re.match(r'^https?://', url))
    
    @staticmethod
    def parse_channel_line(line: str) -> Optional[Tuple[str, str]]:
        """解析频道行"""
        line = line.strip()
        if not line or line.startswith('#'):
            return None
        
        # 支持多种分隔符
        for sep in [',', '|', '\t']:
            if sep in line:
                parts = line.split(sep, 1)
                if len(parts) == 2:
                    name, url = map(TextUtils.clean_text, parts)
                    if name and url and TextUtils.is_valid_url(url):
                        return name, url
        return None
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """标准化名称"""
        return re.sub(r'[^\w]', '', name.lower())

class NetworkUtils:
    """网络工具"""
    
    @staticmethod
    def create_session() -> requests.Session:
        """创建优化会话"""
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=100,
            max_retries=2
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        session.headers.update({
            'User-Agent': random.choice(AppConfig.USER_AGENTS),
            'Accept': '*/*',
            'Connection': 'keep-alive'
        })
        
        return session
    
    @staticmethod
    def check_connectivity(url: str = "http://www.baidu.com", timeout: int = 5) -> bool:
        """检查网络连接"""
        try:
            response = requests.get(url, timeout=timeout)
            return response.status_code == 200
        except Exception:
            return False

# ======================== 配置管理 =========================
class ConfigManager:
    """配置管理系统"""
    
    DEFAULT_CONFIG = {
        'source': {
            'urls': [
                "https://raw.githubusercontent.com/iptv-org/iptv/master/channels.txt"
            ],
            'min_length': 100
        },
        'performance': {
            'max_fetch_workers': 5,
            'max_speed_workers': 8,
            'connect_timeout': 8,
            'read_timeout': 15
        },
        'resolution': {
            'enable': True,
            'min_width': 1280,
            'min_height': 720
        }
    }
    
    def __init__(self):
        self._config = self.DEFAULT_CONFIG.copy()
        self._config_file = None
    
    def load(self, config_file: str = None) -> bool:
        """加载配置文件"""
        files_to_try = [config_file] if config_file else AppConfig.CONFIG_FILES
        
        for file in files_to_try:
            if os.path.exists(file):
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        if file.endswith('.json'):
                            config = json.load(f)
                        else:
                            config = yaml.safe_load(f)
                    
                    self._merge_configs(config)
                    self._config_file = file
                    Console.print_success(f"配置加载成功: {file}")
                    return True
                except Exception as e:
                    Console.print_error(f"配置加载失败 {file}: {str(e)}")
        
        Console.print_warning("未找到配置文件，使用默认配置")
        return False
    
    def get(self, key: str, default=None) -> Any:
        """获取配置值"""
        keys = key.split('.')
        value = self._config
        for k in keys:
            value = value.get(k, {})
        return value if value != {} else default
    
    def _merge_configs(self, new_config: Dict):
        """深度合并配置"""
        for section, values in new_config.items():
            if section in self._config:
                if isinstance(values, dict):
                    self._config[section].update(values)
                else:
                    self._config[section] = values
            else:
                self._config[section] = values

# ======================== 模板管理 =========================
class TemplateManager:
    """模板管理系统"""
    
    @staticmethod
    def load_template(file_path: str = None) -> List[Dict]:
        """加载模板文件"""
        file = file_path or AppConfig.DEFAULT_TEMPLATE
        if not os.path.exists(file):
            Console.print_warning(f"模板文件不存在: {file}")
            return []
        
        try:
            with open(file, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            Console.print_error(f"加载模板失败: {str(e)}")
            return []
    
    @staticmethod
    def parse_template(lines: List[str]) -> Dict[str, List[str]]:
        """解析模板内容"""
        categories = {}
        current_category = None
        
        for line in lines:
            if '#genre#' in line:
                current_category = line.split(',')[0].strip()
                categories[current_category] = []
            elif current_category and line:
                categories[current_category].append(line)
        
        return categories

# ======================== 性能监控 =========================
class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self._metrics = {
            'cpu': [],
            'memory': [],
            'network': []
        }
        self._running = False
        self._thread = None
    
    def start(self):
        """开始监控"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        Console.print_info("性能监控已启动")
    
    def stop(self):
        """停止监控"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        Console.print_info("性能监控已停止")
    
    def _monitor_loop(self):
        """监控循环"""
        while self._running:
            self._record_metrics()
            time.sleep(1)
    
    def _record_metrics(self):
        """记录性能指标"""
        self._metrics['cpu'].append(psutil.cpu_percent())
        self._metrics['memory'].append(psutil.virtual_memory().percent)
        
        net_io = psutil.net_io_counters()
        self._metrics['network'].append({
            'sent': net_io.bytes_sent,
            'recv': net_io.bytes_recv
        })
    
    def generate_report(self) -> Dict:
        """生成性能报告"""
        if not self._metrics['cpu']:
            return {}
        
        return {
            'cpu_avg': statistics.mean(self._metrics['cpu']),
            'cpu_max': max(self._metrics['cpu']),
            'memory_avg': statistics.mean(self._metrics['memory']),
            'memory_max': max(self._metrics['memory']),
            'network_total': {
                'sent': self._metrics['network'][-1]['sent'] - self._metrics['network'][0]['sent'],
                'recv': self._metrics['network'][-1]['recv'] - self._metrics['network'][0]['recv']
            },
            'duration': len(self._metrics['cpu'])
        }

def monitor_performance(func):
    """性能监控装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        monitor = PerformanceMonitor()
        monitor.start()
        
        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            
            report = monitor.generate_report()
            report['function'] = func.__name__
            report['elapsed'] = elapsed
            
            Console.print_info(f"性能报告: {json.dumps(report, indent=2)}")
            return result
        finally:
            monitor.stop()
    
    return wrapper

# ======================== 核心处理器 =========================
class IPTVProcessor:
    """IPTV处理核心"""
    
    def __init__(self):
        self.config = ConfigManager()
        self.session = NetworkUtils.create_session()
        self.stats = ProcessingStats()
    
    @monitor_performance
    def process(self) -> bool:
        """主处理流程"""
        Console.print_separator(f"{AppConfig.NAME} v{AppConfig.VERSION}")
        
        try:
            # 1. 初始化系统
            if not self._initialize():
                return False
            
            # 2. 获取源数据
            sources = self._fetch_sources()
            if not sources:
                return False
            
            # 3. 处理频道
            channels = self._process_channels(sources)
            if not channels:
                return False
            
            # 4. 生成输出
            return self._generate_output(channels)
            
        except KeyboardInterrupt:
            Console.print_warning("用户中断执行")
            return False
        except Exception as e:
            Console.print_error(f"处理失败: {str(e)}")
            logger.exception("处理异常")
            return False
    
    def _initialize(self) -> bool:
        """初始化系统"""
        # 加载配置
        if not self.config.load():
            Console.print_warning("使用默认配置")
        
        # 检查网络
        if not NetworkUtils.check_connectivity():
            Console.print_warning("网络连接检查失败，继续尝试...")
        
        return True
    
    def _fetch_sources(self) -> List[str]:
        """获取源数据"""
        Console.print_separator("抓取源数据")
        
        sources = []
        urls = self.config.get('source.urls', [])
        max_workers = self.config.get('performance.max_fetch_workers', 3)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._fetch_single, url): url 
                for url in urls
            }
            
            for future in as_completed(futures):
                url = futures[future]
                try:
                    content = future.result()
                    if content:
                        sources.append(content)
                        Console.print_success(f"抓取成功: {url[:50]}...")
                        self.stats.valid_sources += 1
                except Exception as e:
                    Console.print_error(f"抓取失败 {url}: {str(e)}")
                
                # 随机延迟防止封禁
                time.sleep(random.uniform(0.1, 0.5))
        
        self.stats.total_sources = len(urls)
        return sources
    
    def _fetch_single(self, url: str) -> Optional[str]:
        """抓取单个源"""
        try:
            timeout = (
                self.config.get('performance.connect_timeout', 8),
                self.config.get('performance.read_timeout', 15)
            )
            
            response = self.session.get(url, timeout=timeout)
            if response.status_code == 200:
                content = response.text.strip()
                if len(content) >= self.config.get('source.min_length', 100):
                    return content
                Console.print_warning(f"内容过短: {len(content)}字符")
            else:
                Console.print_warning(f"HTTP {response.status_code}")
        except Exception as e:
            Console.print_error(f"请求异常: {str(e)}")
        
        return None
    
    def _process_channels(self, sources: List[str]) -> List[ChannelInfo]:
        """处理频道数据"""
        Console.print_separator("处理频道数据")
        
        # 解析频道
        all_channels = []
        for content in sources:
            channels = self._parse_channels(content)
            all_channels.extend(channels)
        
        self.stats.total_channels = len(all_channels)
        if not all_channels:
            Console.print_error("未解析到有效频道")
            return []
        
        # 测速筛选
        valid_channels = self._speed_test(all_channels)
        if not valid_channels:
            return []
        
        return valid_channels
    
    def _parse_channels(self, content: str) -> List[ChannelInfo]:
        """解析频道列表"""
        channels = []
        for line in content.splitlines():
            result = TextUtils.parse_channel_line(line)
            if result:
                name, url = result
                channels.append(ChannelInfo(name, url))
        return channels
    
    def _speed_test(self, channels: List[ChannelInfo]) -> List[ChannelInfo]:
        """频道测速"""
        Console.print_info(f"开始测速 ({len(channels)}个频道)...")
        
        valid_channels = []
        max_workers = self.config.get('performance.max_speed_workers', 5)
        timeout = self.config.get('performance.connect_timeout', 8)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._test_single, channel, timeout): channel 
                for channel in channels
            }
            
            for future in as_completed(futures):
                channel = future.result()
                if channel.status == ChannelStatus.VALID:
                    valid_channels.append(channel)
                    Console.print_success(f"{channel.name:<20} | 延迟: {channel.delay:.2f}s")
                else:
                    Console.print_error(f"{channel.name:<20} | 测速失败")
        
        self.stats.speed_tested = len(valid_channels)
        Console.print_success(f"测速完成 | 有效频道: {len(valid_channels)}/{len(channels)}")
        return valid_channels
    
    def _test_single(self, channel: ChannelInfo, timeout: int) -> ChannelInfo:
        """单频道测速"""
        try:
            start = time.time()
            response = self.session.get(
                channel.url,
                timeout=timeout,
                stream=True,
                headers={'User-Agent': random.choice(AppConfig.USER_AGENTS)}
            )
            
            if response.status_code == 200:
                # 读取前10KB计算速度
                content = b""
                for chunk in response.iter_content(1024):
                    content += chunk
                    if len(content) >= 10240:  # 10KB
                        break
                
                elapsed = time.time() - start
                channel.delay = elapsed
                channel.speed = len(content) / elapsed / 1024  # KB/s
                channel.status = ChannelStatus.VALID
        except Exception:
            channel.status = ChannelStatus.TIMEOUT
        
        return channel
    
    def _generate_output(self, channels: List[ChannelInfo]) -> bool:
        """生成输出文件"""
        Console.print_separator("生成输出文件")
        
        try:
            # 生成M3U格式
            m3u_content = ["#EXTM3U"]
            for channel in channels:
                m3u_content.extend([
                    f'#EXTINF:-1 tvg-name="{channel.name}",{channel.name}',
                    channel.url
                ])
            
            # 生成TXT格式
            txt_content = []
            for channel in channels:
                txt_content.append(f"{channel.name},{channel.url}")
            
            # 写入文件
            with open("iptv.m3u", 'w', encoding='utf-8') as f:
                f.write("\n".join(m3u_content))
                
            with open("iptv.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(txt_content))
            
            self.stats.final_channels = len(channels)
            self.stats.end_time = time.time()
            
            Console.print_success("输出文件生成成功")
            self._print_stats()
            return True
            
        except Exception as e:
            Console.print_error(f"输出失败: {str(e)}")
            return False
    
    def _print_stats(self):
        """打印统计信息"""
        Console.print_separator("处理统计")
        Console.print_info(f"源数据: {self.stats.valid_sources}/{self.stats.total_sources} 成功")
        Console.print_info(f"原始频道: {self.stats.total_channels} 个")
        Console.print_info(f"有效频道: {self.stats.speed_tested} 个")
        Console.print_info(f"最终保留: {self.stats.final_channels} 个")
        Console.print_info(f"处理耗时: {self.stats.elapsed_time:.2f} 秒")

# ======================== 主程序 =========================
def main():
    """程序入口"""
    try:
        processor = IPTVProcessor()
        success = processor.process()
        sys.exit(0 if success else 1)
    except Exception as e:
        Console.print_error(f"程序异常: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
