#!/usr/bin/env python3
"""
IPTV源处理工具 - 专业增强版 v20.0
完整单一文件版本 - 所有生成文件在根目录
功能：多源抓取、智能测速、协议支持、模板匹配、质量报告
特点：高性能、模块化、强健壮性、完整监控、边界处理
"""

import os
import sys
import re
import time
import json
import pickle
import hashlib
import logging
import platform
import threading
import argparse
import subprocess
from typing import List, Dict, Tuple, Optional, Any, Set, Generator
from dataclasses import dataclass, field
from enum import Enum, auto
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

# ======================== 依赖检查与兼容性处理 =========================
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("❌ 需要安装 requests: pip install requests")

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    print("❌ 需要安装 pyyaml: pip install pyyaml")

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    from fuzzywuzzy import fuzz
    FUZZYWUZZY_AVAILABLE = True
except ImportError:
    FUZZYWUZZY_AVAILABLE = False

try:
    import colorama
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False

# ======================== 根目录定义 =========================
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# ======================== 数据类型定义 =========================
class StreamType(Enum):
    HLS = "hls"
    HTTP = "http"
    RTMP = "rtmp"
    RTSP = "rtsp"
    UDP = "udp"
    MMS = "mms"
    UNKNOWN = "unknown"

class VideoCodec(Enum):
    H264 = "h264"
    H265 = "h265"
    MPEG4 = "mpeg4"
    UNKNOWN = "unknown"

class AudioCodec(Enum):
    AAC = "aac"
    MP3 = "mp3"
    AC3 = "ac3"
    UNKNOWN = "unknown"

class ResolutionQuality(Enum):
    UHD_8K = auto()
    UHD_4K = auto()
    FHD_1080P = auto()
    HD_720P = auto()
    SD_480P = auto()
    LOW_360P = auto()
    UNKNOWN = auto()

class ChannelStatus(Enum):
    VALID = "valid"
    INVALID = "invalid"
    TIMEOUT = "timeout"
    UNREACHABLE = "unreachable"
    LOW_SPEED = "low_speed"

@dataclass
class StreamQuality:
    video_bitrate: int = 0
    audio_bitrate: int = 0
    total_bitrate: int = 0
    video_codec: VideoCodec = VideoCodec.UNKNOWN
    audio_codec: AudioCodec = AudioCodec.UNKNOWN
    stream_type: StreamType = StreamType.UNKNOWN
    resolution: str = ""
    fps: float = 0.0
    has_video: bool = False
    has_audio: bool = False
    is_live: bool = False

@dataclass
class ChannelInfo:
    name: str
    url: str
    group: str = ""
    language: str = ""
    country: str = ""
    tvg_id: str = ""
    tvg_logo: str = ""
    delay: float = 0.0
    speed: float = 0.0
    width: int = 0
    height: int = 0
    quality: ResolutionQuality = ResolutionQuality.UNKNOWN
    status: ChannelStatus = ChannelStatus.INVALID
    source: str = ""
    last_checked: float = field(default_factory=time.time)
    stream_quality: StreamQuality = field(default_factory=StreamQuality)
    ffmpeg_supported: bool = False
    
    def __post_init__(self):
        self._detect_protocol()
        self._parse_extinf()
        self._update_quality()
        self._validate_fields()
    
    def _validate_fields(self):
        """字段验证和清理"""
        self.name = self.name.strip() if self.name else "未知频道"
        self.url = self.url.strip() if self.url else ""
        self.group = self.group.strip() if self.group else "默认分类"
        
        # 名称长度限制
        if len(self.name) > 200:
            self.name = self.name[:197] + "..."
    
    def _detect_protocol(self):
        """自动检测流媒体协议"""
        if not self.url:
            return
            
        url_lower = self.url.lower()
        if '.m3u8' in url_lower:
            self.stream_quality.stream_type = StreamType.HLS
        elif url_lower.startswith('rtmp://'):
            self.stream_quality.stream_type = StreamType.RTMP
        elif url_lower.startswith('rtsp://'):
            self.stream_quality.stream_type = StreamType.RTSP
        elif url_lower.startswith('udp://'):
            self.stream_quality.stream_type = StreamType.UDP
        elif url_lower.startswith('mms://'):
            self.stream_quality.stream_type = StreamType.MMS
        else:
            self.stream_quality.stream_type = StreamType.HTTP
    
    def _parse_extinf(self):
        """解析M3U格式的EXTINF信息"""
        if '#EXTINF' in self.name:
            try:
                parts = self.name.split(',', 1)
                if len(parts) > 1:
                    # 提取频道名称
                    self.name = parts[1].strip()
                    
                    # 解析属性
                    attrs = re.findall(r'([a-z\-]+)="([^"]+)"', parts[0])
                    for key, value in attrs:
                        if key == 'tvg-id':
                            self.tvg_id = value
                        elif key == 'group-title':
                            self.group = value
                        elif key == 'tvg-logo':
                            self.tvg_logo = value
                        elif key == 'language':
                            self.language = value
                        elif key == 'country':
                            self.country = value
            except Exception:
                pass  # 解析失败时保持原始数据
    
    def _update_quality(self):
        """更新分辨率质量"""
        if self.width >= 7680 or self.height >= 4320:
            self.quality = ResolutionQuality.UHD_8K
        elif self.width >= 3840 or self.height >= 2160:
            self.quality = ResolutionQuality.UHD_4K
        elif self.width >= 1920 or self.height >= 1080:
            self.quality = ResolutionQuality.FHD_1080P
        elif self.width >= 1280 or self.height >= 720:
            self.quality = ResolutionQuality.HD_720P
        elif self.width >= 854 or self.height >= 480:
            self.quality = ResolutionQuality.SD_480P
        elif self.width > 0 and self.height > 0:
            self.quality = ResolutionQuality.LOW_360P
        else:
            self.quality = ResolutionQuality.UNKNOWN
    
    @property
    def is_valid(self):
        return self.status == ChannelStatus.VALID
    
    @property
    def resolution_str(self):
        if self.width > 0 and self.height > 0:
            return f"{self.width}x{self.height}"
        return "未知"
    
    @property
    def bitrate_str(self):
        if self.stream_quality.total_bitrate > 0:
            return f"{self.stream_quality.total_bitrate:.1f} kbps"
        return "未知"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'name': self.name,
            'url': self.url,
            'group': self.group,
            'status': self.status.value,
            'resolution': self.resolution_str,
            'bitrate': self.bitrate_str,
            'protocol': self.stream_quality.stream_type.value,
            'last_checked': datetime.fromtimestamp(self.last_checked).isoformat(),
            'source': self.source
        }

@dataclass
class ProcessingStats:
    total_sources: int = 0
    valid_sources: int = 0
    total_channels: int = 0
    speed_tested: int = 0
    valid_channels: int = 0
    template_matched: int = 0
    final_channels: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    memory_peak: float = 0
    network_errors: int = 0
    cache_hits: int = 0
    retry_attempts: int = 0
    ffmpeg_tests: int = 0
    ffmpeg_success: int = 0
    
    @property
    def elapsed_time(self):
        return (self.end_time or time.time()) - self.start_time
    
    def update_memory_peak(self):
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                self.memory_peak = max(self.memory_peak, memory_mb)
            except Exception:
                pass

# ======================== 控制台输出系统 =========================
class Console:
    COLORS = {
        'green': '\033[92m',
        'red': '\033[91m', 
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'cyan': '\033[96m',
        'magenta': '\033[95m',
        'reset': '\033[0m'
    }
    
    _lock = Lock()
    _progress_length = 50
    _colors_initialized = False
    
    @classmethod
    def _init_colors(cls):
        if cls._colors_initialized:
            return
            
        if platform.system() == "Windows" and COLORAMA_AVAILABLE:
            try:
                colorama.init()
            except Exception:
                cls.COLORS = {k: '' for k in cls.COLORS}
        elif platform.system() == "Windows":
            cls.COLORS = {k: '' for k in cls.COLORS}
        
        cls._colors_initialized = True
    
    @classmethod
    def print(cls, message: str, color: Optional[str] = None, end: str = "\n"):
        cls._init_colors()
        with cls._lock:
            color_code = cls.COLORS.get(color, '')
            reset_code = cls.COLORS['reset']
            if color_code:
                print(f"{color_code}{message}{reset_code}", end=end, flush=True)
            else:
                print(message, end=end, flush=True)
    
    @classmethod
    def print_success(cls, message: str):
        cls.print(f"✅ {message}", 'green')
    
    @classmethod
    def print_error(cls, message: str):
        cls.print(f"❌ {message}", 'red')
    
    @classmethod
    def print_warning(cls, message: str):
        cls.print(f"⚠️ {message}", 'yellow')
    
    @classmethod
    def print_info(cls, message: str):
        cls.print(f"ℹ️ {message}", 'blue')
    
    @classmethod
    def print_ffmpeg(cls, message: str):
        cls.print(f"🎥 {message}", 'magenta')
    
    @classmethod
    def print_progress(cls, current: int, total: int, prefix: str = ""):
        with cls._lock:
            percent = current / total if total > 0 else 0
            filled = int(cls._progress_length * percent)
            bar = '█' * filled + '░' * (cls._progress_length - filled)
            progress = f"\r{prefix} [{bar}] {current}/{total} ({percent:.1%})"
            print(progress, end='', flush=True)
            if current == total:
                print()

# ======================== 增强配置系统 =========================
class EnhancedConfig:
    """增强的配置管理系统，所有文件生成在根目录"""
    
    _instance = None
    _config_file = os.path.join(ROOT_DIR, "config.yaml")
    _template_file = os.path.join(ROOT_DIR, "demo.txt")
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_config()
        return cls._instance
    
    def _init_config(self):
        """初始化配置系统"""
        if not os.path.exists(self._config_file):
            self._create_default_config()
        self._load_config()
    
    def _create_default_config(self):
        """创建默认配置文件"""
        default_config = {
            'version': "20.0",
            'app_name': "IPTV Processor Pro",
            'network': {
                'timeout': 15,
                'max_retries': 3,
                'retry_delay': 2,
                'proxy': None,
                'user_agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                'speed_test_timeout': 10,
                'min_speed_kbps': 500
            },
            'performance': {
                'max_workers': {
                    'source': 8,
                    'speed_test': 6,
                    'parsing': 10
                },
                'min_content_length': 1024,
                'cache_max_age': 3600,
                'max_cache_size': 1000
            },
            'sources': [
                "https://raw.githubusercontent.com/iptv-org/iptv/master/channels.txt",
                "https://mirror.ghproxy.com/https://raw.githubusercontent.com/iptv-org/iptv/master/channels.txt",
                "https://fastly.jsdelivr.net/gh/iptv-org/iptv@master/channels.txt",
            ],
            'files': {
                'output_txt': os.path.join(ROOT_DIR, "iptv.txt"),
                'output_m3u': os.path.join(ROOT_DIR, "iptv.m3u"),
                'quality_report': os.path.join(ROOT_DIR, "quality_report.json"),
                'log_file': os.path.join(ROOT_DIR, "iptv_processor.log")
            },
            'streaming': {
                'supported_protocols': ['http', 'https', 'hls', 'rtmp', 'rtsp', 'udp', 'mms'],
                'test_duration': 10,
                'buffer_size': 8192
            }
        }
        
        try:
            with open(self._config_file, 'w', encoding='utf-8') as f:
                yaml.safe_dump(default_config, f, allow_unicode=True)
            Console.print_success(f"默认配置文件已创建: {self._config_file}")
        except Exception as e:
            raise RuntimeError(f"创建配置文件失败: {e}")
    
    def _load_config(self):
        """加载配置文件"""
        try:
            with open(self._config_file, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            
            # 环境变量覆盖
            if os.getenv('IPTV_PROXY'):
                self._config['network']['proxy'] = os.getenv('IPTV_PROXY')
            if os.getenv('IPTV_TIMEOUT'):
                try:
                    self._config['network']['timeout'] = int(os.getenv('IPTV_TIMEOUT'))
                except ValueError:
                    Console.print_warning(f"无效的超时设置: {os.getenv('IPTV_TIMEOUT')}")
                    
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败: {e}")
    
    def __getattr__(self, name):
        """动态获取配置项"""
        if name in self._config:
            return self._config[name]
        raise AttributeError(f"Config has no attribute '{name}'")
    
    def get_output_path(self, key: str) -> str:
        """获取输出文件路径"""
        return self._config['files'][key]
    
    @property
    def template_file(self):
        """获取模板文件路径"""
        return self._template_file

# ======================== 工具类 =========================
class TextUtils:
    """文本处理工具类"""
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """文本标准化处理"""
        if not text or not isinstance(text, str):
            return ""
        return re.sub(r'\s+', ' ', text.strip())
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """URL有效性验证"""
        if not url or not isinstance(url, str) or len(url) > 1000:
            return False
        try:
            result = urlparse(url)
            return all([
                result.scheme in ['http', 'https', 'rtmp', 'rtsp', 'udp', 'mms'], 
                result.netloc,
                len(result.netloc) <= 253
            ])
        except Exception:
            return False
    
    @staticmethod
    def parse_channel_line(line: str) -> Optional[Tuple[str, str]]:
        """解析频道行，支持多种格式"""
        if not line or not isinstance(line, str) or len(line) > 5000:
            return None
            
        line = TextUtils.normalize_text(line)
        if not line or line.startswith('##'):
            return None
        
        # 处理M3U格式（在解析器中特殊处理）
        if line.startswith('#EXTINF'):
            return None
        
        # 多种分隔符支持
        patterns = [
            (r'^([^,]+?),\s*(https?://[^\s]+)$', '标准格式'),
            (r'^([^|]+?)\|\s*(https?://[^\s]+)$', '竖线分隔'),
            (r'^([^\t]+?)\t(https?://[^\s]+)$', '制表符分隔'),
        ]
        
        for pattern, _ in patterns:
            try:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    name = TextUtils.normalize_text(match.group(1))
                    url = TextUtils.normalize_text(match.group(2))
                    if name and url and TextUtils.is_valid_url(url):
                        return name, url
            except Exception:
                continue
        
        return None

class ResolutionDetector:
    """分辨率检测器"""
    
    @staticmethod
    def detect_from_name(channel_name: str) -> Tuple[int, int, str]:
        """从频道名称检测分辨率"""
        if not channel_name or not isinstance(channel_name, str):
            return 1280, 720, "auto"
        
        try:
            channel_lower = channel_name.lower()
            
            # 精确分辨率匹配
            match = re.search(r'(\d{3,4})[×xX*](\d{3,4})', channel_lower)
            if match:
                width, height = int(match.group(1)), int(match.group(2))
                if 100 <= width <= 7680 and 100 <= height <= 4320:
                    return width, height, f"{width}x{height}"
            
            # 标准分辨率匹配
            if any(x in channel_lower for x in ['8k', '4320p']):
                return 7680, 4320, "8K"
            elif any(x in channel_lower for x in ['4k', 'uhd', '2160p']):
                return 3840, 2160, "4K"
            elif any(x in channel_lower for x in ['1080p', 'fhd', '全高清']):
                return 1920, 1080, "1080P"
            elif any(x in channel_lower for x in ['720p', 'hd', '高清']):
                return 1280, 720, "720P"
            elif any(x in channel_lower for x in ['480p', 'sd', '标清']):
                return 854, 480, "480P"
            elif any(x in channel_lower for x in ['360p', 'low']):
                return 640, 360, "360P"
                
        except Exception:
            pass
        
        return 1280, 720, "auto"

class TemplateManager:
    """模板管理器"""
    
    @staticmethod
    def load_template(file_path: str = None) -> List[str]:
        """加载模板文件"""
        config = EnhancedConfig()
        template_file = file_path or config.template_file
        
        if not os.path.exists(template_file):
            return TemplateManager._create_default_template(template_file)
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip()]
            Console.print_success(f"模板加载成功: {len(lines)}行")
            return lines
        except Exception as e:
            Console.print_error(f"模板加载失败: {str(e)}")
            return []
    
    @staticmethod
    def _create_default_template(file_path: str) -> List[str]:
        """创建默认模板文件"""
        try:
            default_content = """# 默认IPTV模板文件 (demo.txt)
# 格式：频道名称,URL 或 #EXTINF格式

#genre#中央台
CCTV-1综合,http://example.com/cctv1
CCTV-2财经,http://example.com/cctv2
CCTV-5体育,http://example.com/cctv5

#genre#卫视台
湖南卫视,http://example.com/hunan
浙江卫视,http://example.com/zhejiang

#genre#国际台
BBC News,http://example.com/bbc
CNN International,http://example.com/cnn"""
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(default_content)
            
            Console.print_info(f"已创建默认模板文件: {file_path}")
            return [line.strip() for line in default_content.splitlines() if line.strip()]
        except Exception as e:
            Console.print_error(f"创建模板失败: {e}")
            return []
    
    @staticmethod
    def parse_template_structure(lines: List[str]) -> Dict[str, List[str]]:
        """解析模板结构"""
        structure = {}
        current_category = "默认分类"
        
        for line in lines:
            if not line or not isinstance(line, str):
                continue
                
            line = line.strip()
            if not line or line.startswith('##'):
                continue
                
            if '#genre#' in line:
                current_category = line.split(',')[0].replace('#genre#', '').strip()
                if not current_category:
                    current_category = "未分类"
                structure[current_category] = []
            elif current_category and line and not line.startswith('#'):
                channel_name = line.split(',')[0].strip()
                if channel_name:
                    if current_category not in structure:
                        structure[current_category] = []
                    structure[current_category].append(channel_name)
        
        return structure

# ======================== 增强缓存管理器 =========================
class EnhancedCacheManager:
    """智能缓存管理器"""
    
    def __init__(self):
        self.config = EnhancedConfig()
        self.cache_dir = os.path.join(ROOT_DIR, ".iptv_cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        self._lock = Lock()
        self.disabled = False
    
    def _get_cache_file(self, key: str) -> str:
        """获取缓存文件路径"""
        safe_key = re.sub(r'[^\w\-_]', '_', key)
        return os.path.join(self.cache_dir, f"{safe_key}.pkl")
    
    def get(self, key: str, max_age: int = None) -> Optional[Any]:
        """获取缓存数据"""
        if self.disabled:
            return None
            
        cache_file = self._get_cache_file(key)
        max_age = max_age or self.config.performance['cache_max_age']
        
        with self._lock:
            if not os.path.exists(cache_file):
                return None
            
            try:
                file_age = time.time() - os.path.getmtime(cache_file)
                if file_age > max_age:
                    os.remove(cache_file)
                    return None
                
                with open(cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
                
                # 验证数据完整性
                if isinstance(cache_data, dict) and 'data' in cache_data and 'expire' in cache_data:
                    if time.time() > cache_data['expire']:
                        os.remove(cache_file)
                        return None
                    return cache_data['data']
                else:
                    os.remove(cache_file)
                    return None
                    
            except Exception as e:
                Console.print_warning(f"缓存读取失败 {key}: {e}")
                try:
                    os.remove(cache_file)
                except:
                    pass
                return None
    
    def set(self, key: str, data: Any, expire: int = None) -> bool:
        """设置缓存数据"""
        if self.disabled:
            return False
            
        cache_file = self._get_cache_file(key)
        expire_time = time.time() + (expire or self.config.performance['cache_max_age'])
        
        with self._lock:
            try:
                cache_data = {
                    'data': data,
                    'expire': expire_time,
                    'created': time.time()
                }
                
                with open(cache_file, 'wb') as f:
                    pickle.dump(cache_data, f)
                
                self._clean_old_cache()
                return True
            except Exception as e:
                Console.print_warning(f"缓存写入失败 {key}: {e}")
                return False
    
    def _clean_old_cache(self):
        """LRU缓存清理"""
        if self.disabled:
            return
            
        try:
            cache_files = [f for f in os.listdir(self.cache_dir) if f.endswith('.pkl')]
            if len(cache_files) <= self.config.performance['max_cache_size']:
                return
            
            # 按修改时间排序
            file_stats = []
            for f in cache_files:
                try:
                    file_path = os.path.join(self.cache_dir, f)
                    file_stats.append((file_path, os.path.getmtime(file_path)))
                except Exception:
                    continue
            
            file_stats.sort(key=lambda x: x[1])
            
            # 保留最新的N个文件
            keep_count = self.config.performance['max_cache_size']
            for f, _ in file_stats[:-keep_count]:
                try:
                    os.remove(f)
                except Exception:
                    continue
                    
        except Exception as e:
            Console.print_warning(f"缓存清理失败: {e}")

# ======================== 资源监控器 =========================
class ResourceMonitor:
    """系统资源监控器"""
    
    def __init__(self, processor):
        self.processor = processor
        self._stop_event = Event()
        self._thread = None
        self.max_memory_mb = 1024
        self._degraded = False
    
    def start(self):
        """启动资源监控"""
        if not PSUTIL_AVAILABLE:
            Console.print_warning("psutil不可用，跳过资源监控")
            return
            
        def monitor():
            while not self._stop_event.is_set():
                try:
                    self.processor.stats.update_memory_peak()
                    
                    # 内存使用监控
                    mem_usage = psutil.Process().memory_info().rss / 1024 / 1024
                    
                    # 内存超限时自动降级
                    if mem_usage > self.max_memory_mb and not self._degraded:
                        self._reduce_workload()
                        self._degraded = True
                    
                    time.sleep(5)
                except Exception:
                    break
        
        self._thread = threading.Thread(target=monitor, daemon=True)
        self._thread.start()
        Console.print_info("资源监控已启动")
    
    def stop(self):
        """停止监控"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1)
        Console.print_info("资源监控已停止")
    
    def _reduce_workload(self):
        """内存超限时自动降级处理"""
        config = self.processor.config
        
        # 降低并发数
        old_workers = config.performance['max_workers']['speed_test']
        new_workers = max(1, old_workers // 2)
        config.performance['max_workers']['speed_test'] = new_workers
        
        # 禁用缓存
        if hasattr(self.processor, 'cache_manager'):
            self.processor.cache_manager.disabled = True
        
        Console.print_warning(
            f"内存使用超过 {self.max_memory_mb}MB，自动降级: "
            f"并发数 {old_workers}->{new_workers}, 禁用缓存"
        )

# ======================== 增强网络管理器 =========================
class EnhancedNetworkManager:
    """增强的网络管理器"""
    
    def __init__(self):
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests库未安装")
            
        self.config = EnhancedConfig()
        self.session = self._create_session()
        self.cache = {}
        self._cache_lock = Lock()
    
    def _create_session(self) -> requests.Session:
        """创建配置好的请求会话"""
        session = requests.Session()
        
        # 重试策略
        retry_strategy = Retry(
            total=self.config.network['max_retries'],
            backoff_factor=self.config.network['retry_delay'],
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=50,
            pool_maxsize=100
        )
        
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        # 代理设置
        if self.config.network['proxy']:
            session.proxies = {
                'http': self.config.network['proxy'],
                'https': self.config.network['proxy']
            }
        
        return session
    
    def fetch(self, url: str, use_cache: bool = True) -> Optional[str]:
        """增强的抓取方法"""
        if not TextUtils.is_valid_url(url):
            Console.print_warning(f"无效的URL: {url}")
            return None
            
        cache_key = f"source_{hashlib.md5(url.encode()).hexdigest()}"
        
        if use_cache:
            with self._cache_lock:
                if url in self.cache:
                    return self.cache[url]
        
        try:
            headers = {
                'User-Agent': self.config.network['user_agent'],
                'Accept': 'text/plain,text/html,*/*',
                'Accept-Encoding': 'gzip, deflate'
            }
            
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.config.network['timeout'],
                stream=False
            )
            response.raise_for_status()
            
            content = response.text
            if len(content) >= self.config.performance['min_content_length']:
                if use_cache:
                    with self._cache_lock:
                        self.cache[url] = content
                return content
            else:
                Console.print_warning(f"内容过短: {url} ({len(content)} bytes)")
                
        except requests.exceptions.RequestException as e:
            Console.print_warning(f"网络请求失败 {url}: {str(e)}")
        except Exception as e:
            Console.print_warning(f"抓取异常 {url}: {str(e)}")
        
        return None
    
    def test_speed(self, url: str) -> Dict[str, Any]:
        """增强的测速方法"""
        if not TextUtils.is_valid_url(url):
            return {
                'url': url,
                'status': 'failed',
                'delay': 0,
                'speed_kbps': 0,
                'valid': False,
                'error': 'invalid_url'
            }
            
        start_time = time.time()
        metrics = {
            'url': url,
            'status': 'failed',
            'delay': 0,
            'speed_kbps': 0,
            'valid': False,
            'error': None
        }
        
        try:
            headers = {
                'User-Agent': self.config.network['user_agent'],
                'Range': 'bytes=0-102399'
            }
            
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.config.network['speed_test_timeout'],
                stream=True
            )
            
            if response.status_code in (200, 206):
                content_length = 0
                start_read = time.time()
                
                for chunk in response.iter_content(self.config.streaming['buffer_size']):
                    content_length += len(chunk)
                    if content_length >= 102400:  # 100KB
                        break
                    if time.time() - start_read > self.config.network['speed_test_timeout']:
                        break
                
                total_time = time.time() - start_time
                speed_kbps = (content_length / total_time) / 1024 if total_time > 0 else 0
                
                metrics.update({
                    'status': 'success',
                    'delay': total_time,
                    'speed_kbps': speed_kbps,
                    'valid': speed_kbps >= self.config.network['min_speed_kbps']
                })
            else:
                metrics['error'] = f"HTTP {response.status_code}"
                
        except requests.exceptions.Timeout:
            metrics['error'] = 'timeout'
        except requests.exceptions.RequestException as e:
            metrics['error'] = str(e)
        except Exception as e:
            metrics['error'] = f"unexpected error: {str(e)}"
        
        return metrics

# ======================== 主处理器 =========================
class EnhancedIPTVProcessor:
    """增强的IPTV处理器"""
    
    def __init__(self):
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests 库未安装，请运行: pip install requests")
        if not YAML_AVAILABLE:
            raise ImportError("pyyaml 库未安装，请运行: pip install pyyaml")
            
        self.config = EnhancedConfig()
        self.network = EnhancedNetworkManager()
        self.cache_manager = EnhancedCacheManager()
        self.resource_monitor = ResourceMonitor(self)
        self.stats = ProcessingStats()
        self._stop_event = Event()
        self._setup_logging()
    
    def _setup_logging(self):
        """设置日志系统"""
        logger = logging.getLogger('IPTV_Processor')
        logger.setLevel(logging.INFO)
        
        # 清除已有处理器
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 文件处理器
        try:
            log_file = self.config.get_output_path('log_file')
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            Console.print_warning(f"无法创建日志文件: {e}")
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        self.logger = logger
    
    def _check_existing_files(self):
        """检查是否已存在输出文件"""
        files_to_check = [
            self.config.get_output_path('output_txt'),
            self.config.get_output_path('output_m3u')
        ]
        
        existing_files = []
        for file_path in files_to_check:
            if os.path.exists(file_path):
                existing_files.append(file_path)
        
        if existing_files:
            Console.print_warning("以下文件已存在:")
            for f in existing_files:
                Console.print_warning(f"  - {f}")
            
            confirm = input("是否覆盖？(y/n): ").strip().lower()
            if confirm != 'y':
                Console.print_info("取消操作")
                return False
        
        return True
    
    def _fetch_with_cache(self) -> List[str]:
        """带缓存的多源抓取"""
        Console.print_info("开始多源抓取（带缓存）...")
        
        cached_sources = []
        fresh_sources = []
        self.stats.total_sources = len(self.config.sources)
        
        for i, url in enumerate(self.config.sources, 1):
            if self._stop_event.is_set():
                break
                
            cache_key = f"source_{hashlib.md5(url.encode()).hexdigest()}"
            cached_content = self.cache_manager.get(cache_key)
            
            if cached_content:
                cached_sources.append(cached_content)
                self.stats.cache_hits += 1
                Console.print_success(f"[{i}/{len(self.config.sources)}] 缓存命中: {url}")
            else:
                content = self.network.fetch(url, use_cache=False)
                if content:
                    self.cache_manager.set(cache_key, content)
                    fresh_sources.append(content)
                    self.stats.valid_sources += 1
                    Console.print_success(f"[{i}/{len(self.config.sources)}] 抓取成功: {url}")
                else:
                    Console.print_warning(f"[{i}/{len(self.config.sources)}] 抓取失败: {url}")
            
            Console.print_progress(i, len(self.config.sources), "源抓取进度")
        
        all_sources = cached_sources + fresh_sources
        Console.print_info(f"源抓取完成: {len(all_sources)}/{len(self.config.sources)} (缓存: {len(cached_sources)})")
        return all_sources
    
    def _parse_channels_enhanced(self, sources: List[str]) -> Generator[ChannelInfo, None, None]:
        """增强的频道解析器"""
        seen_urls = set()
        channel_count = 0
        
        for i, content in enumerate(sources, 1):
            if self._stop_event.is_set():
                break
                
            if not content or not isinstance(content, str):
                continue
                
            channels_from_source = 0
            lines = content.splitlines()
            j = 0
            
            while j < len(lines):
                if self._stop_event.is_set():
                    break
                    
                line = lines[j].strip()
                if not line:
                    j += 1
                    continue
                
                # 处理M3U格式
                if line.startswith('#EXTINF'):
                    if j + 1 < len(lines):
                        extinf_line = line
                        url_line = lines[j + 1].strip()
                        
                        if url_line and not url_line.startswith('#') and TextUtils.is_valid_url(url_line):
                            if url_line not in seen_urls:
                                seen_urls.add(url_line)
                                
                                try:
                                    # 解析EXTINF
                                    name_match = re.search(r'#EXTINF:.*?,(.+)', extinf_line)
                                    if name_match:
                                        name = name_match.group(1).strip()
                                        channel = ChannelInfo(name=name, url=url_line, source=f"Source_{i}")
                                        
                                        # 解析EXTINF属性
                                        attrs = re.findall(r'([a-z\-]+)="([^"]+)"', extinf_line)
                                        for key, value in attrs:
                                            if key == 'group-title':
                                                channel.group = value
                                            elif key == 'tvg-id':
                                                channel.tvg_id = value
                                            elif key == 'tvg-logo':
                                                channel.tvg_logo = value
                                        
                                        width, height, _ = ResolutionDetector.detect_from_name(name)
                                        channel.width = width
                                        channel.height = height
                                        
                                        channels_from_source += 1
                                        channel_count += 1
                                        yield channel
                                except Exception as e:
                                    Console.print_warning(f"解析M3U频道失败: {e}")
                            
                            j += 2  # 跳过URL行
                            continue
                
                # 处理标准格式
                result = TextUtils.parse_channel_line(line)
                if result:
                    name, url = result
                    if url not in seen_urls:
                        seen_urls.add(url)
                        channel = ChannelInfo(name=name, url=url, source=f"Source_{i}")
                        
                        width, height, _ = ResolutionDetector.detect_from_name(name)
                        channel.width = width
                        channel.height = height
                        
                        channels_from_source += 1
                        channel_count += 1
                        yield channel
                
                j += 1
            
            if channels_from_source > 0:
                Console.print_info(f"源{i}: 解析{channels_from_source}个频道")
        
        self.stats.total_channels = channel_count
        Console.print_success(f"频道解析完成: {channel_count}个频道")
    
    def _speed_test_channels(self, channels: List[ChannelInfo]) -> List[ChannelInfo]:
        """并发测速频道"""
        Console.print_info("开始频道测速...")
        
        valid_channels = []
        
        with ThreadPoolExecutor(
            max_workers=self.config.performance['max_workers']['speed_test']
        ) as executor:
            futures = {
                executor.submit(self._test_single_channel, channel): channel 
                for channel in channels
            }
            
            for i, future in enumerate(as_completed(futures), 1):
                if self._stop_event.is_set():
                    break
                    
                channel = futures[future]
                try:
                    tested_channel = future.result(timeout=self.config.network['speed_test_timeout'] + 5)
                    if tested_channel.is_valid:
                        valid_channels.append(tested_channel)
                    
                    if i % 5 == 0 or i == len(channels) or i <= 10:
                        Console.print_progress(i, len(channels), "测速进度")
                        
                except Exception as e:
                    Console.print_warning(f"测速失败 {channel.name}: {e}")
        
        self.stats.speed_tested = len(valid_channels)
        Console.print_success(f"测速完成: {len(valid_channels)}/{len(channels)}个有效")
        return valid_channels
    
    def _test_single_channel(self, channel: ChannelInfo) -> ChannelInfo:
        """测试单个频道"""
        metrics = self.network.test_speed(channel.url)
        
        channel.delay = metrics['delay']
        channel.speed = metrics['speed_kbps']
        channel.last_checked = time.time()
        
        if metrics['valid']:
            channel.status = ChannelStatus.VALID
            if channel.speed > 1000:  # 只显示高速频道
                Console.print_success(
                    f"{channel.name:<25} | "
                    f"{channel.stream_quality.stream_type.value.upper():<6} | "
                    f"延迟:{channel.delay:5.2f}s | "
                    f"速度:{channel.speed:6.1f}KB/s"
                )
        else:
            channel.status = ChannelStatus.LOW_SPEED
            self.stats.network_errors += 1
        
        return channel
    
    def _fuzzy_template_matching(self, channels: List[ChannelInfo]) -> List[ChannelInfo]:
        """模板匹配"""
        Console.print_info("开始模板匹配...")
        
        template_lines = TemplateManager.load_template(self.config.template_file)
        if not template_lines:
            Console.print_warning("无模板文件，返回所有有效频道")
            return channels
        
        template_structure = TemplateManager.parse_template_structure(template_lines)
        if not template_structure:
            Console.print_warning("模板解析为空，返回所有有效频道")
            return channels
        
        template_names = set()
        for category_channels in template_structure.values():
            template_names.update([name.lower().strip() for name in category_channels if name.strip()])
        
        Console.print_info(f"模板频道数: {len(template_names)}")
        
        matched_channels = []
        exact_matches = 0
        
        for i, channel in enumerate(channels, 1):
            channel_name_lower = channel.name.lower().strip()
            if channel_name_lower in template_names:
                matched_channels.append(channel)
                exact_matches += 1
            
            if i % 50 == 0 or i == len(channels):
                Console.print_progress(i, len(channels), "模板匹配进度")
        
        Console.print_success(f"精确匹配: {exact_matches}/{len(channels)}")
        
        # 模糊匹配
        if exact_matches == 0 and FUZZYWUZZY_AVAILABLE:
            Console.print_info("尝试模糊匹配...")
            fuzzy_matches = 0
            fuzzy_threshold = 80
            
            for i, channel in enumerate(channels, 1):
                if channel in matched_channels:
                    continue
                    
                channel_name_lower = channel.name.lower().strip()
                best_score = 0
                
                for template_name in template_names:
                    score = fuzz.token_sort_ratio(channel_name_lower, template_name)
                    if score > fuzzy_threshold and score > best_score:
                        best_score = score
                
                if best_score >= fuzzy_threshold:
                    matched_channels.append(channel)
                    fuzzy_matches += 1
                
                if i % 20 == 0 or i == len(channels):
                    Console.print_progress(i, len(channels), "模糊匹配进度")
            
            Console.print_success(f"模糊匹配: {fuzzy_matches}个")
        elif exact_matches == 0:
            Console.print_warning("fuzzywuzzy 未安装，跳过模糊匹配")
        
        self.stats.template_matched = len(matched_channels)
        Console.print_success(f"模板匹配完成: {len(matched_channels)}/{len(channels)}")
        return matched_channels
    
    def _generate_txt_content(self, channels: List[ChannelInfo]) -> str:
        """生成TXT格式内容"""
        template_lines = TemplateManager.load_template(self.config.template_file)
        
        if not template_lines:
            # 按组分类
            groups = {}
            for channel in channels:
                group = channel.group or "默认分类"
                if group not in groups:
                    groups[group] = []
                groups[group].append(channel)
        else:
            groups = TemplateManager.parse_template_structure(template_lines)
            # 按模板结构组织频道
            organized_channels = {}
            for category, names in groups.items():
                organized_channels[category] = [
                    c for c in channels 
                    if c.name.lower() in [n.lower() for n in names]
                ]
            groups = organized_channels
        
        lines = [
            f"# IPTV频道列表 - {self.config.app_name} v{self.config.version}",
            f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"# 总频道数: {len(channels)}",
            f"# 协议支持: {', '.join(self.config.streaming['supported_protocols'])}",
            ""
        ]
        
        for category, category_channels in groups.items():
            if category_channels:
                lines.append(f"{category},#genre#")
                
                # 按速度排序
                category_channels.sort(key=lambda x: x.speed, reverse=True)
                
                for channel in category_channels:
                    lines.append(f"{channel.name},{channel.url}")
                lines.append("")
        
        return "\n".join(lines)
    
    def _generate_m3u_content(self, channels: List[ChannelInfo]) -> str:
        """生成M3U格式内容"""
        lines = ["#EXTM3U"]
        
        # 按组分类
        groups = {}
        for channel in channels:
            group = channel.group or "默认分类"
            if group not in groups:
                groups[group] = []
            groups[group].append(channel)
        
        for group, group_channels in groups.items():
            group_channels.sort(key=lambda x: x.speed, reverse=True)
            
            for channel in group_channels:
                extinf_line = f'#EXTINF:-1 tvg-id="{channel.tvg_id}" tvg-name="{channel.name}"'
                extinf_line += f' tvg-logo="{channel.tvg_logo}" group-title="{group}"'
                extinf_line += f',{channel.name}'
                
                lines.append(extinf_line)
                lines.append(channel.url)
        
        return "\n".join(lines)
    
    def _generate_quality_report(self, channels: List[ChannelInfo]) -> Dict[str, Any]:
        """生成质量报告数据"""
        return {
            'metadata': {
                'app': self.config.app_name,
                'version': self.config.version,
                'generated_at': datetime.now().isoformat(),
                'processing_stats': {
                    'total_sources': self.stats.total_sources,
                    'valid_sources': self.stats.valid_sources,
                    'total_channels': self.stats.total_channels,
                    'valid_channels': self.stats.speed_tested,
                    'final_channels': self.stats.final_channels,
                    'elapsed_time': self.stats.elapsed_time,
                    'memory_peak_mb': self.stats.memory_peak,
                    'network_errors': self.stats.network_errors,
                    'cache_hits': self.stats.cache_hits
                }
            },
            'channels': [channel.to_dict() for channel in channels]
        }
    
    def _generate_outputs(self, channels: List[ChannelInfo]) -> bool:
        """在根目录生成所有输出文件"""
        try:
            # 生成TXT文件
            txt_file = self.config.get_output_path('output_txt')
            with open(txt_file, 'w', encoding='utf-8') as f:
                f.write(self._generate_txt_content(channels))
            
            # 生成M3U文件
            m3u_file = self.config.get_output_path('output_m3u')
            with open(m3u_file, 'w', encoding='utf-8') as f:
                f.write(self._generate_m3u_content(channels))
            
            # 生成质量报告
            report_file = self.config.get_output_path('quality_report')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(self._generate_quality_report(channels), f, ensure_ascii=False, indent=2)
            
            Console.print_success("文件已生成到根目录:")
            Console.print_success(f"频道列表: {txt_file}")
            Console.print_success(f"M3U文件: {m3u_file}")
            Console.print_success(f"质量报告: {report_file}")
            return True
        except Exception as e:
            Console.print_error(f"文件生成失败: {e}")
            return False
    
    def _print_final_stats(self):
        """打印最终统计"""
        Console.print_success("🎉 处理完成！")
        Console.print_info(f"⏱️ 处理耗时: {self.stats.elapsed_time:.2f}秒")
        Console.print_info(f"💾 内存峰值: {self.stats.memory_peak:.1f}MB")
        Console.print_info(f"🌐 有效源: {self.stats.valid_sources}/{self.stats.total_sources}")
        Console.print_info(f"📺 总频道: {self.stats.total_channels}")
        Console.print_info(f"⚡ 测速有效: {self.stats.speed_tested}")
        Console.print_info(f"🔍 模板匹配: {self.stats.template_matched}")
        Console.print_info(f"📤 最终输出: {self.stats.final_channels}")
        Console.print_info(f"❌ 网络错误: {self.stats.network_errors}")
        Console.print_info(f"💿 缓存命中: {self.stats.cache_hits}")
    
    def process(self) -> bool:
        """主处理流程"""
        Console.print_success(f"🚀 {self.config.app_name} v{self.config.version} 开始处理")
        
        try:
            # 检查文件覆盖
            if not self._check_existing_files():
                return False
            
            self.resource_monitor.start()
            
            # 1. 带缓存的多源抓取
            sources_content = self._fetch_with_cache()
            if not sources_content:
                Console.print_error("❌ 无有效源数据")
                return False
            
            # 2. 流式解析频道
            all_channels = list(self._parse_channels_enhanced(sources_content))
            if not all_channels:
                Console.print_error("❌ 无有效频道数据")
                return False
            
            # 3. 测速验证
            valid_channels = self._speed_test_channels(all_channels)
            if not valid_channels:
                Console.print_error("❌ 无有效频道通过测速")
                return False
            
            # 4. 模板匹配
            final_channels = self._fuzzy_template_matching(valid_channels)
            if not final_channels:
                Console.print_warning("⚠️ 无频道匹配模板，使用所有有效频道")
                final_channels = valid_channels
            
            # 5. 生成输出
            success = self._generate_outputs(final_channels)
            
            if success:
                self.stats.final_channels = len(final_channels)
                self._print_final_stats()
            
            return success
            
        except KeyboardInterrupt:
            Console.print_warning("⏹️ 用户中断处理")
            return False
        except Exception as e:
            Console.print_error(f"💥 处理异常: {str(e)}")
            return False
        finally:
            self.stats.end_time = time.time()
            self._stop_event.set()
            self.resource_monitor.stop()
            if hasattr(self, 'network') and hasattr(self.network, 'session'):
                self.network.session.close()

# ======================== 完整性验证 =========================
def validate_integrity():
    """验证代码完整性"""
    tests = [
        ("配置系统", lambda: EnhancedConfig().network['timeout'] == 15),
        ("缓存管理", lambda: EnhancedCacheManager().set('test', {'data': 1})),
        ("网络管理", lambda: hasattr(EnhancedNetworkManager(), 'test_speed')),
        ("文本工具", lambda: TextUtils.parse_channel_line("CCTV-1,http://test.com") is not None),
        ("分辨率检测", lambda: ResolutionDetector.detect_from_name("CCTV-4K")[0] == 3840),
        ("模板管理", lambda: len(TemplateManager.load_template()) > 0),
    ]
    
    results = []
    for name, test in tests:
        try:
            success = test()
            results.append((name, success))
        except Exception as e:
            results.append((name, False, str(e)))
    
    print("\n" + "="*50)
    print("完整性验证结果:")
    print("="*50)
    
    all_passed = True
    for name, success, *extra in results:
        status = "✅" if success else "❌"
        print(f"{status} {name}", *extra)
        if not success:
            all_passed = False
    
    print("="*50)
    if all_passed:
        print("🎉 所有组件验证通过！")
    else:
        print("⚠️ 部分组件验证失败")
    
    return all_passed

# ======================== 主程序入口 =========================
def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='IPTV源处理工具 - 专业增强版',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python iptv_processor.py                    # 默认运行
  python iptv_processor.py --validate        # 验证完整性
  python iptv_processor.py --check-deps      # 检查依赖
  python iptv_processor.py --template custom.txt  # 自定义模板
  python iptv_processor.py --proxy http://proxy:8080 # 使用代理
        """
    )
    
    parser.add_argument('--validate', action='store_true', help='验证代码完整性')
    parser.add_argument('--check-deps', action='store_true', help='检查依赖')
    parser.add_argument('--template', type=str, help='模板文件路径')
    parser.add_argument('--proxy', type=str, help='HTTP代理服务器')
    parser.add_argument('--verbose', action='store_true', help='详细输出')
    parser.add_argument('--version', action='store_true', help='显示版本')
    
    args = parser.parse_args()
    
    if args.version:
        config = EnhancedConfig()
        print(f"{config.app_name} v{config.version}")
        return
    
    if args.validate:
        sys.exit(0 if validate_integrity() else 1)
    
    if args.check_deps:
        missing = []
        for package in ['requests', 'yaml']:
            try:
                if package == 'yaml':
                    __import__('yaml')
                else:
                    __import__(package)
                print(f"✅ {package}")
            except ImportError:
                print(f"❌ {package}")
                missing.append(package)
        
        if missing:
            print(f"\n安装命令: pip install {' '.join(missing)}")
            sys.exit(1)
        else:
            print("\n✅ 所有依赖已安装")
            sys.exit(0)
    
    try:
        # 应用命令行参数
        config = EnhancedConfig()
        
        if args.template:
            if not os.path.exists(args.template):
                Console.print_error(f"模板文件不存在: {args.template}")
                return
            config._template_file = args.template
        
        if args.proxy:
            config.network['proxy'] = args.proxy
        
        # 创建处理器
        processor = EnhancedIPTVProcessor()
        
        if args.verbose:
            processor.logger.setLevel(logging.DEBUG)
        
        # 运行处理
        success = processor.process()
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        Console.print_warning("用户中断程序")
        sys.exit(1)
    except Exception as e:
        Console.print_error(f"程序异常: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
