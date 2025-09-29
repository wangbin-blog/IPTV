#!/usr/bin/env python3
"""
IPTV源处理工具 - 终极完整版
版本：4.0
功能：多源抓取、智能测速、严格过滤、模板匹配、性能监控、配置管理
作者：终极优化版
"""

import requests
import re
import os
import time
import json
import logging
import random
import hashlib
import platform
import sys
import threading
import statistics
import psutil
from typing import List, Dict, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum, auto
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, RLock
from pathlib import Path
from urllib.parse import urlparse
import yaml

# ======================== 日志配置 =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('iptv_processor.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('IPTV_Processor')

# ======================== 数据类型定义 =========================
class ResolutionQuality(Enum):
    """分辨率质量等级"""
    UHD_4K = auto()
    FHD_1080P = auto()
    HD_720P = auto()
    SD_480P = auto()
    LOW_360P = auto()
    UNKNOWN = auto()
    LOW_QUALITY = auto()

class ChannelStatus(Enum):
    """频道状态枚举"""
    VALID = auto()
    INVALID = auto()
    TIMEOUT = auto()
    UNREACHABLE = auto()

@dataclass
class ChannelInfo:
    """频道信息数据类"""
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
    source_hash: str = ""

@dataclass
class CategoryInfo:
    """分类信息数据类"""
    name: str
    channels: List[str] = field(default_factory=list)
    marker: str = ""

@dataclass
class TemplateStructure:
    """模板结构数据类"""
    type: str  # 'category' or 'channel'
    name: str
    category: Optional[str] = None
    line_num: int = 0

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
        """计算处理耗时"""
        return (self.end_time or time.time()) - self.start_time

@dataclass
class PerformanceMetrics:
    """性能指标"""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    network_io: Dict
    disk_io: Dict
    active_threads: int
    processing_speed: float = 0.0
    channels_processed: int = 0

@dataclass
class ProcessStats:
    """进程统计"""
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    total_channels: int = 0
    valid_channels: int = 0
    avg_processing_time: float = 0
    peak_memory: float = 0
    total_network_io: int = 0
    
    @property
    def duration(self) -> float:
        return (self.end_time or time.time()) - self.start_time
    
    @property
    def success_rate(self) -> float:
        return (self.valid_channels / self.total_channels * 100) if self.total_channels > 0 else 0

# ======================== 配置管理系统 =========================
@dataclass
class PerformanceConfig:
    """性能配置类"""
    max_fetch_workers: int = 5
    max_speed_test_workers: int = 10
    max_resolution_workers: int = 8
    speed_test_timeout: int = 10
    connect_timeout: int = 8
    read_timeout: int = 15
    cache_expire: int = 3600
    max_cache_size: int = 200
    request_interval: list = None
    
    def __post_init__(self):
        if self.request_interval is None:
            self.request_interval = [0.2, 0.3, 0.4, 0.5]

@dataclass
class ResolutionConfig:
    """分辨率配置类"""
    enable: bool = True
    min_width: int = 1280
    min_height: int = 720
    strict_mode: bool = False
    remove_low_resolution: bool = True
    low_res_threshold: tuple = (854, 480)
    timeout: int = 10
    keep_unknown: bool = False

@dataclass
class TemplateConfig:
    """模板配置类"""
    input_file: str = "demo.txt"
    backup_file: str = "demo_backup.txt"
    txt_output: str = "iptv.txt"
    m3u_output: str = "iptv.m3u"
    category_marker: str = "#genre#"

@dataclass
class SourceConfig:
    """源配置类"""
    urls: list = None
    test_url: str = "http://www.baidu.com"
    min_content_length: int = 100
    
    def __post_init__(self):
        if self.urls is None:
            self.urls = [
                "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
                "https://live.zbds.top/tv/iptv6.txt",
                "https://live.zbds.top/tv/iptv4.txt",
            ]

class ConfigManager:
    """配置管理器 - 支持热更新"""
    
    CONFIG_FILES = ['config.json', 'config.yaml', 'config.yml']
    
    def __init__(self):
        self.performance = PerformanceConfig()
        self.resolution = ResolutionConfig()
        self.template = TemplateConfig()
        self.source = SourceConfig()
        self._custom_configs = {}
        self._config_file = None
        
    def load_config(self, config_file: Optional[str] = None) -> bool:
        """加载配置文件"""
        if config_file:
            files_to_try = [config_file]
        else:
            files_to_try = self.CONFIG_FILES
            
        for file_path in files_to_try:
            if os.path.exists(file_path):
                try:
                    if file_path.endswith('.json'):
                        config_data = self._load_json(file_path)
                    else:
                        config_data = self._load_yaml(file_path)
                    
                    self._apply_config(config_data)
                    self._config_file = file_path
                    logger.info(f"配置文件加载成功: {file_path}")
                    return True
                except Exception as e:
                    logger.error(f"配置文件加载失败 {file_path}: {e}")
        
        logger.warning("未找到配置文件，使用默认配置")
        return False
    
    def save_config(self, config_file: str = "config.json") -> bool:
        """保存配置文件"""
        try:
            config_data = {
                'performance': self._dataclass_to_dict(self.performance),
                'resolution': self._dataclass_to_dict(self.resolution),
                'template': self._dataclass_to_dict(self.template),
                'source': self._dataclass_to_dict(self.source),
                'custom': self._custom_configs
            }
            
            with open(config_file, 'w', encoding='utf-8') as f:
                if config_file.endswith('.json'):
                    json.dump(config_data, f, indent=2, ensure_ascii=False)
                else:
                    yaml.safe_dump(config_data, f, default_flow_style=False, allow_unicode=True)
            
            logger.info(f"配置文件保存成功: {config_file}")
            return True
        except Exception as e:
            logger.error(f"配置文件保存失败: {e}")
            return False
    
    def update_config(self, section: str, key: str, value: Any) -> bool:
        """动态更新配置"""
        try:
            if section == 'performance':
                setattr(self.performance, key, value)
            elif section == 'resolution':
                setattr(self.resolution, key, value)
            elif section == 'template':
                setattr(self.template, key, value)
            elif section == 'source':
                setattr(self.source, key, value)
            else:
                self._custom_configs[key] = value
            
            logger.info(f"配置更新: {section}.{key} = {value}")
            return True
        except Exception as e:
            logger.error(f"配置更新失败: {e}")
            return False
    
    def get_config(self, section: str, key: str) -> Any:
        """获取配置值"""
        if section == 'performance':
            return getattr(self.performance, key, None)
        elif section == 'resolution':
            return getattr(self.resolution, key, None)
        elif section == 'template':
            return getattr(self.template, key, None)
        elif section == 'source':
            return getattr(self.source, key, None)
        else:
            return self._custom_configs.get(key)
    
    def _load_json(self, file_path: str) -> Dict:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _load_yaml(self, file_path: str) -> Dict:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _apply_config(self, config_data: Dict):
        """应用配置数据"""
        if 'performance' in config_data:
            for key, value in config_data['performance'].items():
                if hasattr(self.performance, key):
                    setattr(self.performance, key, value)
        
        if 'resolution' in config_data:
            for key, value in config_data['resolution'].items():
                if hasattr(self.resolution, key):
                    setattr(self.resolution, key, value)
        
        if 'template' in config_data:
            for key, value in config_data['template'].items():
                if hasattr(self.template, key):
                    setattr(self.template, key, value)
        
        if 'source' in config_data:
            for key, value in config_data['source'].items():
                if hasattr(self.source, key):
                    setattr(self.source, key, value)
        
        if 'custom' in config_data:
            self._custom_configs.update(config_data['custom'])
    
    def _dataclass_to_dict(self, obj):
        """将dataclass转换为字典"""
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return {}

# ======================== 控制台输出工具 =========================
class Console:
    """控制台输出工具类"""
    
    COLORS = {
        'green': '\033[92m',
        'red': '\033[91m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'cyan': '\033[96m',
        'magenta': '\033[95m',
        'reset': '\033[0m'
    }
    
    print_lock = Lock()
    
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
    def print(cls, message: str, color: str = None, icon: str = "", end: str = "\n"):
        """线程安全的彩色输出"""
        with cls.print_lock:
            color_code = cls.COLORS.get(color, '')
            reset_code = cls.COLORS['reset']
            formatted_msg = f"{icon} {message}" if icon else message
            if color_code:
                print(f"{color_code}{formatted_msg}{reset_code}", end=end)
            else:
                print(formatted_msg, end=end)
    
    @classmethod
    def print_success(cls, message: str):
        """成功信息"""
        cls.print(message, 'green', '✅')
        logger.info(f"SUCCESS: {message}")
    
    @classmethod
    def print_error(cls, message: str):
        """错误信息"""
        cls.print(message, 'red', '❌')
        logger.error(f"ERROR: {message}")
    
    @classmethod
    def print_warning(cls, message: str):
        """警告信息"""
        cls.print(message, 'yellow', '⚠️')
        logger.warning(f"WARNING: {message}")
    
    @classmethod
    def print_info(cls, message: str):
        """信息提示"""
        cls.print(message, 'blue', '🔍')
        logger.info(f"INFO: {message}")
    
    @classmethod
    def print_separator(cls, title: str = "", length: int = 70):
        """打印分隔线"""
        with cls.print_lock:
            sep = "=" * length
            if title:
                print(f"\n{sep}\n📌 {cls.COLORS['blue']}{title}{cls.COLORS['reset']}\n{sep}")
            else:
                print(sep)

# 初始化控制台颜色
Console._init_colors()

# ======================== 性能监控系统 =========================
class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self, update_interval: float = 1.0):
        self.update_interval = update_interval
        self.metrics: List[PerformanceMetrics] = []
        self.stats = ProcessStats()
        self._running = False
        self._monitor_thread = None
        self._lock = threading.Lock()
    
    def start_monitoring(self):
        """开始监控"""
        self._running = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        Console.print_info("性能监控已启动")
    
    def stop_monitoring(self):
        """停止监控"""
        self._running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        Console.print_info("性能监控已停止")
    
    def record_processing(self, channels_processed: int, processing_time: float):
        """记录处理性能"""
        with self._lock:
            self.stats.total_channels += channels_processed
            self.stats.valid_channels += channels_processed
            self.stats.avg_processing_time = statistics.mean([
                self.stats.avg_processing_time, processing_time
            ]) if self.stats.avg_processing_time > 0 else processing_time
    
    def _monitor_loop(self):
        """监控循环"""
        while self._running:
            try:
                metrics = self._collect_metrics()
                with self._lock:
                    self.metrics.append(metrics)
                    if len(self.metrics) > 1000:
                        self.metrics = self.metrics[-1000:]
                
                time.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"性能监控错误: {e}")
    
    def _collect_metrics(self) -> PerformanceMetrics:
        """收集性能指标"""
        return PerformanceMetrics(
            timestamp=time.time(),
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=psutil.virtual_memory().percent,
            network_io=self._get_network_io(),
            disk_io=self._get_disk_io(),
            active_threads=threading.active_count()
        )
    
    def _get_network_io(self) -> Dict:
        """获取网络IO"""
        net_io = psutil.net_io_counters()
        return {
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv,
            'packets_sent': net_io.packets_sent,
            'packets_recv': net_io.packets_recv
        }
    
    def _get_disk_io(self) -> Dict:
        """获取磁盘IO"""
        disk_io = psutil.disk_io_counters()
        return {
            'read_bytes': disk_io.read_bytes if disk_io else 0,
            'write_bytes': disk_io.write_bytes if disk_io else 0
        }
    
    def generate_report(self) -> Dict:
        """生成性能报告"""
        with self._lock:
            self.stats.end_time = time.time()
            
            if self.metrics:
                cpu_avg = statistics.mean([m.cpu_percent for m in self.metrics])
                memory_avg = statistics.mean([m.memory_percent for m in self.metrics])
                memory_peak = max([m.memory_percent for m in self.metrics])
            else:
                cpu_avg = memory_avg = memory_peak = 0
            
            report = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'duration': self.stats.duration,
                'total_channels': self.stats.total_channels,
                'valid_channels': self.stats.valid_channels,
                'success_rate': self.stats.success_rate,
                'avg_processing_time': self.stats.avg_processing_time,
                'performance_metrics': {
                    'cpu_avg': cpu_avg,
                    'memory_avg': memory_avg,
                    'memory_peak': memory_peak,
                    'total_samples': len(self.metrics)
                },
                'recommendations': self._generate_recommendations()
            }
            
            return report
    
    def _generate_recommendations(self) -> List[str]:
        """生成优化建议"""
        recommendations = []
        
        if self.stats.avg_processing_time > 5:
            recommendations.append("考虑增加并发线程数")
        
        if any(m.cpu_percent > 80 for m in self.metrics):
            recommendations.append("CPU使用率过高，建议减少并发数")
        
        if any(m.memory_percent > 80 for m in self.metrics):
            recommendations.append("内存使用率过高，建议优化缓存策略")
        
        if self.stats.success_rate < 50:
            recommendations.append("成功率较低，建议检查源质量")
        
        return recommendations
    
    def save_report(self, filename: str = "performance_report.json"):
        """保存性能报告"""
        report = self.generate_report()
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        Console.print_success(f"性能报告已保存: {filename}")

# 性能监控装饰器
def monitor_performance(func):
    """性能监控装饰器"""
    def wrapper(*args, **kwargs):
        monitor = PerformanceMonitor()
        monitor.start_monitoring()
        
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            monitor.stop_monitoring()
            processing_time = time.time() - start_time
            monitor.record_processing(100, processing_time)
            monitor.save_report()
    
    return wrapper

# ======================== 核心功能类 =========================
class NetworkUtils:
    """网络工具类"""
    
    @staticmethod
    def check_connectivity() -> bool:
        """检查网络连接"""
        Console.print_info("正在检测网络连接...")
        try:
            timeout = 5 if platform.system() == "Windows" else 3
            response = requests.get("http://www.baidu.com", timeout=timeout)
            if response.status_code == 200:
                Console.print_success(f"网络连接正常（{platform.system()}系统）")
                return True
            else:
                Console.print_error(f"网络检测失败：HTTP状态码 {response.status_code}")
                return False
        except Exception as e:
            Console.print_error(f"网络连接异常：{str(e)}")
            return False
    
    @staticmethod
    def create_session() -> requests.Session:
        """创建优化的请求会话"""
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=100,
            max_retries=2
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "Connection": "keep-alive"
        })
        
        return session

class TextUtils:
    """文本处理工具类"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """清理文本中的多余空格"""
        if not text:
            return ""
        return re.sub(r'^\s+|\s+$|\s+(?=\s)', "", str(text).strip())
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """验证URL格式"""
        return bool(url and re.match(r'^https?://', url))
    
    @staticmethod
    def parse_channel_line(line: str) -> Optional[Tuple[str, str]]:
        """解析频道行"""
        match = re.match(r'([^,]+),(https?://.+)$', line.strip())
        if match:
            name, url = match.groups()
            name = TextUtils.clean_text(name)
            url = TextUtils.clean_text(url)
            if name and url and TextUtils.is_valid_url(url):
                return name, url
        return None

class TemplateManager:
    """模板管理器"""
    
    @staticmethod
    def generate_default_template() -> bool:
        """生成默认模板"""
        default_categories = [
            CategoryInfo("央视频道", ["CCTV1", "CCTV2", "CCTV3", "CCTV5", "CCTV6", "CCTV8", "CCTV13", "CCTV14", "CCTV15"], "央视频道,#genre#"),
            CategoryInfo("卫视频道", ["湖南卫视", "浙江卫视", "东方卫视", "江苏卫视", "北京卫视", "安徽卫视", "深圳卫视", "山东卫视", "天津卫视", "湖北卫视", "广东卫视"], "卫视频道,#genre#"),
            CategoryInfo("地方频道", ["广东卫视", "四川卫视", "湖北卫视", "河南卫视", "河北卫视", "辽宁卫视", "黑龙江卫视"], "地方频道,#genre#"),
            CategoryInfo("高清频道", ["CCTV1高清", "CCTV5高清", "湖南卫视高清", "浙江卫视高清"], "高清频道,#genre#"),
        ]
        
        template_content = [
            f"# IPTV分类模板（自动生成于 {time.strftime('%Y-%m-%d %H:%M:%S')}）",
            f"# 系统：{platform.system()} | 格式说明：分类行（分类名,#genre#）、频道行（纯频道名）",
            f"# 注意：只保留模板内明确列出的频道，不包含其他任何频道",
            ""
        ]
        
        for category in default_categories:
            template_content.extend([
                category.marker,
                *[channel for channel in category.channels],
                ""
            ])
        
        try:
            with open("demo.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(template_content))
            Console.print_success("默认模板生成成功")
            return True
        except Exception as e:
            Console.print_error(f"生成默认模板失败：{str(e)}")
            return False
    
    @staticmethod
    def read_template_strict() -> Tuple[Optional[List[CategoryInfo]], Optional[List[str]], Optional[List[TemplateStructure]]]:
        """严格读取模板"""
        if not os.path.exists("demo.txt"):
            Console.print_warning("分类模板不存在，自动生成...")
            if not TemplateManager.generate_default_template():
                return None, None, None
        
        categories = []
        current_category = None
        all_channels = []
        template_structure = []
        
        try:
            with open("demo.txt", 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f.readlines()]
            
            for line_num, line in enumerate(lines, 1):
                if not line or (line.startswith("#") and "#genre#" not in line):
                    continue
                
                # 处理分类行
                if "#genre#" in line:
                    parts = [p.strip() for p in line.split("#genre#") if p.strip()]
                    cat_name = parts[0] if parts else ""
                    if not cat_name:
                        Console.print_warning(f"第{line_num}行：分类名为空，忽略")
                        current_category = None
                        continue
                    
                    template_structure.append(TemplateStructure("category", cat_name, line_num=line_num))
                    
                    existing_cat = next((c for c in categories if c.name == cat_name), None)
                    if existing_cat:
                        current_category = cat_name
                    else:
                        categories.append(CategoryInfo(cat_name, [], f"{cat_name},#genre#"))
                        current_category = cat_name
                    continue
                
                # 处理频道行
                if current_category is None:
                    Console.print_warning(f"第{line_num}行：频道未分类，跳过（不保留未分类频道）")
                    continue
                
                channel_name = TextUtils.clean_text(line.split(",")[0])
                if not channel_name:
                    Console.print_warning(f"第{line_num}行：频道名为空，忽略")
                    continue
                
                template_structure.append(TemplateStructure("channel", channel_name, current_category, line_num))
                
                current_cat_channels = next(c.channels for c in categories if c.name == current_category)
                if channel_name not in current_cat_channels:
                    current_cat_channels.append(channel_name)
                    if channel_name not in all_channels:
                        all_channels.append(channel_name)
        
        except Exception as e:
            Console.print_error(f"读取模板失败：{str(e)}")
            return None, None, None
        
        # 输出统计
        total_channels = sum(len(c.channels) for c in categories)
        Console.print_success(f"模板读取完成 | 分类数：{len(categories)} | 总频道数：{total_channels}")
        
        return categories, all_channels, template_structure

class IPTVProcessor:
    """IPTV处理器主类"""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.monitor = PerformanceMonitor()
        self.session = NetworkUtils.create_session()
        self.stats = ProcessingStats()
    
    @monitor_performance
    def process(self) -> bool:
        """主处理流程"""
        Console.print_separator("🎬 IPTV源处理工具启动 - 终极完整版")
        
        try:
            # 1. 配置管理
            if not self.config_manager.load_config():
                Console.print_warning("使用默认配置")
            
            # 2. 网络检查
            if not NetworkUtils.check_connectivity():
                Console.print_warning("网络检查失败，继续尝试处理...")
            
            # 3. 读取模板
            Console.print_separator("📋 读取模板")
            template_categories, all_template_channels, template_structure = TemplateManager.read_template_strict()
            if not template_structure:
                return False
            
            # 4. 抓取源数据
            Console.print_separator("🌐 抓取源数据")
            sources_content = self._fetch_all_sources()
            if not sources_content:
                Console.print_error("未获取到有效源数据")
                return False
            
            # 5. 解析频道
            Console.print_separator("📋 解析频道")
            all_channels = []
            for content in sources_content:
                channels = self._parse_channels(content)
                all_channels.extend(channels)
            
            Console.print_success(f"解析完成 | 原始频道数：{len(all_channels)}")
            if not all_channels:
                return False
            
            # 6. 测速筛选
            Console.print_separator("⚡ 频道测速")
            valid_channels = self._speed_test_channels(all_channels)
            if not valid_channels:
                Console.print_error("无有效频道通过测速")
                return False
            
            # 7. 模板匹配
            Console.print_separator("🔍 模板匹配")
            template_filtered_channels = self._filter_by_template(valid_channels, all_template_channels)
            if not template_filtered_channels:
                Console.print_error("无频道匹配模板要求")
                return False
            
            # 8. 生成输出
            Console.print_separator("💾 生成输出")
            success = self._generate_output(template_filtered_channels, template_structure)
            if not success:
                return False
            
            # 9. 显示统计
            self.stats.end_time = time.time()
            self.stats.final_channels = len(template_filtered_channels)
            
            Console.print_separator("📊 最终统计")
            Console.print_info(f"处理统计：")
            Console.print(f"  ├─ 源数据: {self.stats.valid_sources}/{self.stats.total_sources} 成功")
            Console.print(f"  ├─ 原始频道: {self.stats.total_channels} 个")
            Console.print(f"  ├─ 测速有效: {self.stats.speed_tested} 个")
            Console.print(f"  ├─ 模板匹配: {self.stats.template_matched} 个")
            Console.print(f"  ├─ 最终保留: {self.stats.final_channels} 个")
            Console.print(f"  └─ 处理耗时: {self.stats.elapsed_time:.2f} 秒")
            
            Console.print_success("🎉 IPTV处理完成！")
            return True
            
        except KeyboardInterrupt:
            Console.print_warning("用户中断程序执行")
            return False
        except Exception as e:
            Console.print_error(f"程序异常：{str(e)}")
            logger.exception("主程序异常")
            return False
    
    def _fetch_all_sources(self) -> List[str]:
        """并发抓取所有源"""
        sources_content = []
        max_workers = self.config_manager.get_config('performance', 'max_fetch_workers') or 5
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {
                executor.submit(self._fetch_single_source, url): url 
                for url in self.config_manager.source.urls
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    content = future.result()
                    if content:
                        sources_content.append(content)
                        self.stats.valid_sources += 1
                except Exception as e:
                    Console.print_error(f"抓取异常：{url} - {str(e)}")
                
                time.sleep(random.choice([0.2, 0.3, 0.4, 0.5]))
        
        self.stats.total_sources = len(self.config_manager.source.urls)
        return sources_content
    
    def _fetch_single_source(self, url: str) -> Optional[str]:
        """抓取单个源"""
        Console.print_info(f"开始抓取：{url[:50]}{'...' if len(url)>50 else ''}")
        
        try:
            timeout = self.config_manager.get_config('performance', 'connect_timeout') or 8
            response = self.session.get(url, timeout=timeout)
            
            if response.status_code == 200:
                content = response.text.strip()
                min_len = self.config_manager.get_config('source', 'min_content_length') or 100
                if len(content) >= min_len:
                    Console.print_success(f"抓取成功：{url[:50]}{'...' if len(url)>50 else ''}")
                    return content
                else:
                    Console.print_warning(f"内容过短：{url[:50]}{'...' if len(url)>50 else ''}（{len(content)}字符）")
            else:
                Console.print_warning(f"HTTP错误 {response.status_code}：{url[:50]}{'...' if len(url)>50 else ''}")
                
        except Exception as e:
            Console.print_error(f"抓取失败：{url[:50]}{'...' if len(url)>50 else ''} - {str(e)}")
        
        return None
    
    def _parse_channels(self, content: str) -> List[Tuple[str, str]]:
        """从内容解析频道列表"""
        channels = []
        for line in content.splitlines():
            result = TextUtils.parse_channel_line(line)
            if result:
                channels.append(result)
        return channels
    
    def _speed_test_channels(self, channels: List[Tuple[str, str]]) -> List[ChannelInfo]:
        """并发测速频道"""
        Console.print_info(f"开始测速（{len(channels)}个频道）...")
        
        valid_channels = []
        max_workers = self.config_manager.get_config('performance', 'max_speed_test_workers') or 10
        timeout = self.config_manager.get_config('performance', 'speed_test_timeout') or 10
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_channel = {
                executor.submit(self._speed_test_single, channel, timeout): channel 
                for channel in channels
            }
            
            for future in as_completed(future_to_channel):
                channel_info = future.result()
                if channel_info.delay < float('inf'):
                    valid_channels.append(channel_info)
                    Console.print_success(f"{channel_info.name:<15} | 延迟: {channel_info.delay:.2f}s | 速度: {channel_info.speed:.1f} KB/s")
                else:
                    Console.print_error(f"{channel_info.name:<15} | 测速失败")
        
        self.stats.speed_tested = len(valid_channels)
        self.stats.total_channels = len(channels)
        Console.print_success(f"测速完成 | 有效频道: {len(valid_channels)}/{len(channels)}")
        return valid_channels
    
    def _speed_test_single(self, channel_data: Tuple[str, str], timeout: int) -> ChannelInfo:
        """单频道测速"""
        name, url = channel_data
        channel_info = ChannelInfo(name, url, float('inf'), 0.0)
        
        if not TextUtils.is_valid_url(url):
            return channel_info
        
        try:
            start_time = time.time()
            response = requests.get(url, timeout=timeout, stream=True)
            
            if response.status_code == 200:
                content = b""
                for chunk in response.iter_content(chunk_size=1024):
                    content += chunk
                    if len(content) >= 10240:  # 10KB
                        break
                elapsed = time.time() - start_time
                speed = len(content) / elapsed / 1024 if elapsed > 0 else 0
                channel_info.delay = elapsed
                channel_info.speed = speed
        except Exception:
            pass  # 测速失败是正常情况
        
        return channel_info
    
    def _filter_by_template(self, valid_channels: List[ChannelInfo], template_channels: List[str]) -> List[ChannelInfo]:
        """按模板过滤频道"""
        Console.print_info("开始按模板严格过滤频道...")
        
        filtered_channels = []
        matched_count = 0
        
        for template_channel in template_channels:
            # 查找匹配的源频道
            matched_source_channels = []
            for source_channel in valid_channels:
                if template_channel in source_channel.name or source_channel.name in template_channel:
                    matched_source_channels.append(source_channel)
            
            if matched_source_channels:
                # 选择最佳匹配
                matched_source_channels.sort(key=lambda x: x.delay)
                best_channel = matched_source_channels[0]
                best_channel.name = template_channel  # 使用模板中的频道名
                filtered_channels.append(best_channel)
                matched_count += 1
                Console.print_success(f"模板匹配: {template_channel}")
            else:
                Console.print_warning(f"未找到匹配: {template_channel}")
        
        self.stats.template_matched = matched_count
        Console.print_info(f"模板匹配统计：成功匹配 {matched_count}/{len(template_channels)}")
        return filtered_channels
    
    def _generate_output(self, channels: List[ChannelInfo], template_structure: List[TemplateStructure]) -> bool:
        """生成输出文件"""
        # 生成TXT格式
        txt_lines = [
            f"# IPTV频道列表（生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}）",
            f"# 总频道数：{len(channels)}",
            f"# 严格按照模板排序，只保留模板内频道，不包含其他频道",
            ""
        ]
        
        current_category = None
        for item in template_structure:
            if item.type == "category":
                current_category = item.name
                txt_lines.append(f"{current_category},#genre#")
            elif item.type == "channel":
                channel_name = item.name
                channel_data = next((ch for ch in channels if ch.name == channel_name), None)
                if channel_data:
                    txt_lines.append(f"{channel_data.name},{channel_data.url}")
        
        # 生成M3U格式
        m3u_lines = ["#EXTM3U"]
        current_category = None
        for item in template_structure:
            if item.type == "category":
                current_category = item.name
            elif item.type == "channel":
                channel_name = item.name
                channel_data = next((ch for ch in channels if ch.name == channel_name), None)
                if channel_data and current_category:
                    m3u_lines.extend([
                        f'#EXTINF:-1 group-title="{current_category}",{channel_data.name}',
                        channel_data.url
                    ])
        
        # 写入文件
        try:
            with open("iptv.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(txt_lines))
            with open("iptv.m3u", 'w', encoding='utf-8') as f:
                f.write("\n".join(m3u_lines))
            
            Console.print_success("输出文件生成成功")
            return True
        except Exception as e:
            Console.print_error(f"输出文件生成失败：{str(e)}")
            return False

def main():
    """程序入口点"""
    try:
        processor = IPTVProcessor()
        success = processor.process()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        Console.print_warning("用户中断程序执行")
        sys.exit(1)
    except Exception as e:
        Console.print_error(f"程序异常：{str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
