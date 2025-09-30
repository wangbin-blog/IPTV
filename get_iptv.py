#!/usr/bin/env python3
"""
IPTV源处理工具 - 终极优化版 v18.4
功能：多源抓取、智能测速(FFmpeg)、分辨率过滤、严格模板匹配、纯净输出
特点：高性能、低内存、强健壮性、完整监控、极致优化、FFmpeg集成
版本：18.4
修复：网络错误统计、进度显示优化、模板处理逻辑、资源清理
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
import socket
import hashlib
import pickle
import subprocess
import tempfile
import signal
from typing import List, Dict, Tuple, Optional, Any, Union, Generator
from dataclasses import dataclass, field
from enum import Enum, auto
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, RLock, Event
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from functools import lru_cache
import requests

# ======================== 可选依赖处理 =========================
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("提示: 安装 psutil 可获得系统监控功能: pip install psutil")

try:
    from fuzzywuzzy import fuzz
    FUZZYWUZZY_AVAILABLE = True
except ImportError:
    FUZZYWUZZY_AVAILABLE = False
    print("提示: 安装 fuzzywuzzy 可获得模糊匹配功能: pip install fuzzywuzzy python-levenshtein")

try:
    import colorama
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    print("提示: 安装 colorama 可在Windows获得更好的颜色支持: pip install colorama")

# ======================== 配置系统 =========================
class Config:
    """集中配置管理"""
    # 应用信息
    VERSION = "18.4"
    APP_NAME = "IPTV Processor Ultimate"
    
    # 网络配置
    REQUEST_TIMEOUT = (6, 12)
    SPEED_TEST_TIMEOUT = 15
    CONNECT_TIMEOUT = 6
    READ_TIMEOUT = 12
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    
    # 并发配置
    MAX_WORKERS_SOURCE = 8
    MAX_WORKERS_SPEED_TEST = 6  # FFmpeg资源消耗大，降低并发
    MAX_WORKERS_PARSING = 10
    
    # 性能阈值
    MIN_SPEED_KBPS = 100  # 最低速度 100KB/s
    MIN_CONTENT_LENGTH = 1000  # 最小内容长度
    CACHE_MAX_AGE = 3600  # 缓存最大年龄(秒)
    
    # FFmpeg配置
    FFMPEG_TIMEOUT = 20  # FFmpeg检测超时时间
    FFMPEG_ANALYZE_DURATION = 10  # 分析时长(秒)
    FFMPEG_PROBE_SIZE = 5000000  # 探测大小(5MB)
    MIN_VIDEO_BITRATE = 100  # 最小视频码率(kbps)
    MIN_AUDIO_BITRATE = 32   # 最小音频码率(kbps)
    
    # 源列表
    SOURCE_URLS = [
        "https://raw.githubusercontent.com/iptv-org/iptv/master/channels.txt",
        "https://mirror.ghproxy.com/https://raw.githubusercontent.com/iptv-org/iptv/master/channels.txt", 
        "https://fastly.jsdelivr.net/gh/iptv-org/iptv@master/channels.txt",
        "https://raw.fastgit.org/iptv-org/iptv/master/channels.txt",
    ]
    
    # 文件配置
    TEMPLATE_FILE = "demo.txt"
    OUTPUT_TXT = "iptv.txt"
    OUTPUT_M3U = "iptv.m3u"
    OUTPUT_QUALITY_REPORT = "quality_report.json"
    CACHE_DIR = ".iptv_cache"
    LOG_FILE = "iptv_processor.log"
    
    # 模板匹配
    FUZZY_MATCH_THRESHOLD = 80  # 模糊匹配阈值

# ======================== 日志配置 =========================
class LogConfig:
    """日志配置管理"""
    @staticmethod
    def setup_logging():
        """配置日志系统"""
        logger = logging.getLogger('IPTV_Processor')
        logger.setLevel(logging.INFO)
        
        # 清除已有处理器
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 文件处理器
        file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8', mode='w')
        file_handler.setFormatter(formatter)
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

# 初始化全局logger
logger = LogConfig.setup_logging()

# ======================== 数据类型定义 =========================
class StreamType(Enum):
    """流媒体类型"""
    HLS = "hls"
    HTTP = "http"
    RTMP = "rtmp"
    RTSP = "rtsp"
    UDP = "udp"
    UNKNOWN = "unknown"

class VideoCodec(Enum):
    """视频编码"""
    H264 = "h264"
    H265 = "h265"
    MPEG4 = "mpeg4"
    MPEG2 = "mpeg2"
    VP9 = "vp9"
    AV1 = "av1"
    UNKNOWN = "unknown"

class AudioCodec(Enum):
    """音频编码"""
    AAC = "aac"
    MP3 = "mp3"
    AC3 = "ac3"
    OPUS = "opus"
    UNKNOWN = "unknown"

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
    LOW_SPEED = auto()
    DNS_ERROR = auto()
    FORMAT_ERROR = auto()
    CODEC_ERROR = auto()

@dataclass
class StreamQuality:
    """流媒体质量信息"""
    video_bitrate: int = 0  # kbps
    audio_bitrate: int = 0  # kbps
    total_bitrate: int = 0  # kbps
    video_codec: VideoCodec = VideoCodec.UNKNOWN
    audio_codec: AudioCodec = AudioCodec.UNKNOWN
    stream_type: StreamType = StreamType.UNKNOWN
    has_video: bool = False
    has_audio: bool = False
    is_live: bool = False
    duration: float = 0.0
    frame_rate: float = 0.0
    sample_rate: int = 0
    channels: int = 0

@dataclass
class ChannelInfo:
    """频道信息类"""
    name: str
    url: str
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
    connection_time: float = 0.0
    buffer_time: float = 0.0
    
    def __post_init__(self):
        """初始化后自动计算质量等级"""
        self._update_quality()
    
    def _update_quality(self):
        """更新质量等级"""
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
    def is_valid(self) -> bool:
        """检查是否有效"""
        return self.status == ChannelStatus.VALID
    
    @property
    def resolution_str(self) -> str:
        """获取分辨率字符串"""
        if self.width > 0 and self.height > 0:
            return f"{self.width}x{self.height}"
        return "未知"
    
    @property
    def bitrate_str(self) -> str:
        """获取码率字符串"""
        if self.stream_quality.total_bitrate > 0:
            return f"{self.stream_quality.total_bitrate} kbps"
        return "未知"
    
    @property
    def codec_str(self) -> str:
        """获取编码信息字符串"""
        video = self.stream_quality.video_codec.value
        audio = self.stream_quality.audio_codec.value
        return f"{video}+{audio}"

@dataclass
class ProcessingStats:
    """处理统计"""
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
    def elapsed_time(self) -> float:
        return (self.end_time or time.time()) - self.start_time
    
    def update_memory_peak(self):
        """更新内存峰值"""
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                memory_mb = process.memory_info().rss / 1024 / 1024
                self.memory_peak = max(self.memory_peak, memory_mb)
            except Exception:
                pass  # 忽略内存监控错误

# ======================== FFmpeg检测器 =========================
class FFmpegDetector:
    """FFmpeg流媒体检测器 - 完整实现"""
    
    def __init__(self):
        self.ffmpeg_path = self._find_ffmpeg()
        self.ffprobe_path = self._find_ffprobe()
        self._lock = Lock()
    
    def _find_ffmpeg(self) -> Optional[str]:
        """查找FFmpeg可执行文件"""
        possible_paths = [
            'ffmpeg',
            '/usr/bin/ffmpeg',
            '/usr/local/bin/ffmpeg',
            '/opt/homebrew/bin/ffmpeg',
            'C:\\ffmpeg\\bin\\ffmpeg.exe',
            'C:\\Program Files\\ffmpeg\\bin\\ffmpeg.exe',
        ]
        
        return self._check_executable(possible_paths, 'ffmpeg')
    
    def _find_ffprobe(self) -> Optional[str]:
        """查找FFprobe可执行文件"""
        possible_paths = [
            'ffprobe',
            '/usr/bin/ffprobe',
            '/usr/local/bin/ffprobe',
            '/opt/homebrew/bin/ffprobe',
            'C:\\ffmpeg\\bin\\ffprobe.exe',
            'C:\\Program Files\\ffmpeg\\bin\\ffprobe.exe',
        ]
        
        return self._check_executable(possible_paths, 'ffprobe')
    
    def _check_executable(self, paths: List[str], tool_name: str) -> Optional[str]:
        """检查可执行文件是否存在"""
        for path in paths:
            try:
                result = subprocess.run(
                    [path, '-version'],
                    capture_output=True,
                    timeout=5,
                    text=True
                )
                if result.returncode == 0 and tool_name in result.stdout.lower():
                    return path
            except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
                continue
        return None
    
    def is_available(self) -> bool:
        """检查FFmpeg是否可用"""
        return self.ffmpeg_path is not None and self.ffprobe_path is not None
    
    def analyze_stream(self, url: str, timeout: int = Config.FFMPEG_TIMEOUT) -> Optional[Dict[str, Any]]:
        """使用FFprobe分析流媒体"""
        if not self.ffprobe_path:
            return None
        
        try:
            cmd = [
                self.ffprobe_path,
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                '-analyzeduration', '10000000',
                '-probesize', '5000000',
                url
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=timeout,
                text=True
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            else:
                logger.debug(f"FFprobe分析失败: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.debug(f"FFmpeg分析超时: {url}")
            return None
        except json.JSONDecodeError as e:
            logger.debug(f"FFmpeg输出JSON解析失败: {url} - {e}")
            return None
        except Exception as e:
            logger.debug(f"FFmpeg分析异常: {url} - {e}")
            return None
    
    def quick_test_stream(self, url: str, duration: int = 5) -> Optional[Dict[str, Any]]:
        """快速测试流媒体可用性"""
        if not self.ffmpeg_path:
            return None
        
        try:
            cmd = [
                self.ffmpeg_path,
                '-y',  # 覆盖输出文件
                '-t', str(duration),  # 录制时长
                '-i', url,
                '-c', 'copy',  # 直接复制流
                '-f', 'null',  # 输出到空设备
                '-'
            ]
            
            start_time = time.time()
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=duration + 10,
                text=True
            )
            end_time = time.time()
            
            output = {
                'success': result.returncode == 0,
                'duration': end_time - start_time,
                'output': result.stderr,
                'error': result.stderr if result.returncode != 0 else None
            }
            
            # 解析输出信息
            if output['success']:
                output.update({
                    'bitrate': self._parse_bitrate(result.stderr),
                    'speed': self._parse_speed(result.stderr)
                })
            
            return output
            
        except subprocess.TimeoutExpired:
            return {'success': False, 'error': 'timeout', 'duration': duration + 10}
        except Exception as e:
            return {'success': False, 'error': str(e), 'duration': 0}
    
    def _parse_bitrate(self, output: str) -> int:
        """从FFmpeg输出解析码率"""
        patterns = [
            r'bitrate:\s*(\d+)\s*kb/s',
            r'bitrate=(\d+)\s*kb/s',
            r'Video:.*?(\d+)\s*kb/s',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, output)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        return 0
    
    def _parse_speed(self, output: str) -> float:
        """从FFmpeg输出解析速度"""
        match = re.search(r'speed=\s*([\d.]+)x', output)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return 0.0
        return 0.0
    
    def parse_stream_quality(self, probe_data: Dict[str, Any]) -> StreamQuality:
        """解析流媒体质量信息"""
        quality = StreamQuality()
        
        if not probe_data:
            return quality
        
        try:
            streams = probe_data.get('streams', [])
            format_info = probe_data.get('format', {})
            
            # 分析视频流
            video_streams = [s for s in streams if s.get('codec_type') == 'video']
            if video_streams:
                video = video_streams[0]
                quality.has_video = True
                quality.video_codec = self._parse_video_codec(video.get('codec_name', ''))
                
                # 解析码率
                bit_rate = video.get('bit_rate')
                if bit_rate:
                    try:
                        quality.video_bitrate = int(bit_rate) // 1000
                    except (ValueError, TypeError):
                        quality.video_bitrate = 0
                
                # 解析帧率
                r_frame_rate = video.get('r_frame_rate', '0/1')
                quality.frame_rate = self._parse_frame_rate(r_frame_rate)
            
            # 分析音频流
            audio_streams = [s for s in streams if s.get('codec_type') == 'audio']
            if audio_streams:
                audio = audio_streams[0]
                quality.has_audio = True
                quality.audio_codec = self._parse_audio_codec(audio.get('codec_name', ''))
                
                # 解析音频码率
                bit_rate = audio.get('bit_rate')
                if bit_rate:
                    try:
                        quality.audio_bitrate = int(bit_rate) // 1000
                    except (ValueError, TypeError):
                        quality.audio_bitrate = 0
                
                # 解析音频参数
                quality.sample_rate = int(audio.get('sample_rate', 0)) if audio.get('sample_rate') else 0
                quality.channels = int(audio.get('channels', 0)) if audio.get('channels') else 0
            
            # 总码率
            format_bit_rate = format_info.get('bit_rate')
            if format_bit_rate:
                try:
                    quality.total_bitrate = int(format_bit_rate) // 1000
                except (ValueError, TypeError):
                    quality.total_bitrate = 0
            
            # 流类型检测
            format_name = format_info.get('format_name', '')
            quality.stream_type = self._detect_stream_type(format_name)
            
            # 直播流检测
            quality.is_live = self._is_live_stream(format_info)
            
        except Exception as e:
            logger.debug(f"解析流质量信息异常: {e}")
        
        return quality
    
    def _parse_video_codec(self, codec_name: str) -> VideoCodec:
        """解析视频编码"""
        codec_name = codec_name.lower()
        if any(x in codec_name for x in ['h264', 'avc']):
            return VideoCodec.H264
        elif any(x in codec_name for x in ['h265', 'hevc']):
            return VideoCodec.H265
        elif 'mpeg4' in codec_name:
            return VideoCodec.MPEG4
        elif 'mpeg2' in codec_name:
            return VideoCodec.MPEG2
        elif 'vp9' in codec_name:
            return VideoCodec.VP9
        elif 'av1' in codec_name:
            return VideoCodec.AV1
        else:
            return VideoCodec.UNKNOWN
    
    def _parse_audio_codec(self, codec_name: str) -> AudioCodec:
        """解析音频编码"""
        codec_name = codec_name.lower()
        if 'aac' in codec_name:
            return AudioCodec.AAC
        elif 'mp3' in codec_name:
            return AudioCodec.MP3
        elif 'ac3' in codec_name:
            return AudioCodec.AC3
        elif 'opus' in codec_name:
            return AudioCodec.OPUS
        else:
            return AudioCodec.UNKNOWN
    
    def _parse_frame_rate(self, frame_rate: str) -> float:
        """解析帧率"""
        try:
            if '/' in frame_rate:
                num, den = frame_rate.split('/')
                if float(den) != 0:
                    return float(num) / float(den)
            return float(frame_rate)
        except (ValueError, ZeroDivisionError):
            return 0.0
    
    def _detect_stream_type(self, format_name: str) -> StreamType:
        """检测流媒体类型"""
        format_name = format_name.lower()
        if 'hls' in format_name:
            return StreamType.HLS
        elif 'rtmp' in format_name:
            return StreamType.RTMP
        elif 'rtsp' in format_name:
            return StreamType.RTSP
        elif 'udp' in format_name:
            return StreamType.UDP
        elif 'http' in format_name:
            return StreamType.HTTP
        else:
            return StreamType.UNKNOWN
    
    def _is_live_stream(self, format_info: Dict[str, Any]) -> bool:
        """检测是否为直播流"""
        try:
            duration = float(format_info.get('duration', 0))
            return duration < 60  # 小于60秒认为是直播流
        except (ValueError, TypeError):
            return True  # 无法解析duration时默认认为是直播流

# ======================== 缓存系统 =========================
class CacheManager:
    """智能缓存管理系统"""
    
    def __init__(self, cache_dir: str = Config.CACHE_DIR):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self._lock = Lock()
    
    def _get_cache_key(self, data: str) -> str:
        """生成缓存键"""
        return hashlib.md5(data.encode('utf-8')).hexdigest()
    
    def _get_cache_file(self, key: str, suffix: str = ".pkl") -> Path:
        """获取缓存文件路径"""
        return self.cache_dir / f"{key}{suffix}"
    
    def get_cached_data(self, key: str, max_age: int = Config.CACHE_MAX_AGE) -> Optional[Any]:
        """获取缓存数据"""
        cache_file = self._get_cache_file(key)
        
        with self._lock:
            if not cache_file.exists():
                return None
            
            file_age = time.time() - cache_file.stat().st_mtime
            if file_age > max_age:
                cache_file.unlink(missing_ok=True)
                return None
            
            try:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except (pickle.PickleError, EOFError, FileNotFoundError):
                cache_file.unlink(missing_ok=True)
                return None
    
    def set_cached_data(self, key: str, data: Any) -> bool:
        """设置缓存数据"""
        cache_file = self._get_cache_file(key)
        
        with self._lock:
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(data, f)
                return True
            except Exception as e:
                logger.warning(f"缓存写入失败 {key}: {e}")
                return False
    
    def clear_expired_cache(self, max_age: int = Config.CACHE_MAX_AGE):
        """清理过期缓存"""
        with self._lock:
            for cache_file in self.cache_dir.glob("*.pkl"):
                try:
                    file_age = time.time() - cache_file.stat().st_mtime
                    if file_age > max_age:
                        cache_file.unlink(missing_ok=True)
                except Exception:
                    continue
    
    def get_cached_source(self, url: str) -> Optional[str]:
        """获取缓存的源数据"""
        return self.get_cached_data(f"source_{self._get_cache_key(url)}")
    
    def cache_source(self, url: str, content: str) -> bool:
        """缓存源数据"""
        return self.set_cached_data(f"source_{self._get_cache_key(url)}", content)

# ======================== 控制台输出 =========================
class Console:
    """优化控制台输出"""
    
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
        """初始化颜色支持"""
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
    def print(cls, message: str, color: str = None, end: str = "\n"):
        """线程安全打印"""
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
        logger.info(f"SUCCESS: {message}")
    
    @classmethod
    def print_error(cls, message: str):
        cls.print(f"❌ {message}", 'red')
        logger.error(f"ERROR: {message}")
    
    @classmethod
    def print_warning(cls, message: str):
        cls.print(f"⚠️ {message}", 'yellow')
        logger.warning(f"WARNING: {message}")
    
    @classmethod
    def print_info(cls, message: str):
        cls.print(f"ℹ️ {message}", 'blue')
        logger.info(f"INFO: {message}")
    
    @classmethod
    def print_debug(cls, message: str):
        cls.print(f"🔍 {message}", 'cyan')
        logger.debug(f"DEBUG: {message}")
    
    @classmethod
    def print_ffmpeg(cls, message: str):
        cls.print(f"🎥 {message}", 'magenta')
        logger.info(f"FFMPEG: {message}")
    
    @classmethod
    def print_progress(cls, current: int, total: int, prefix: str = ""):
        """优化进度条显示"""
        with cls._lock:
            percent = current / total if total > 0 else 0
            filled = int(cls._progress_length * percent)
            bar = '█' * filled + '░' * (cls._progress_length - filled)
            progress = f"\r{prefix} [{bar}] {current}/{total} ({percent:.1%})"
            print(progress, end='', flush=True)
            if current == total:
                print()

# ======================== 智能分辨率检测器 =========================
class ResolutionDetector:
    """优化分辨率检测器"""
    
    @staticmethod
    def detect_from_name(channel_name: str) -> Tuple[int, int, str]:
        """从频道名称智能检测分辨率"""
        if not channel_name:
            return 1280, 720, "auto"
        
        try:
            channel_lower = channel_name.lower()
            
            # 优先检测数字格式
            match = re.search(r'(\d{3,4})[×xX*](\d{3,4})', channel_lower)
            if match:
                width, height = int(match.group(1)), int(match.group(2))
                if 100 <= width <= 7680 and 100 <= height <= 4320:
                    return width, height, f"{width}x{height}"
            
            # 检测标准分辨率名称
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
                
        except Exception as e:
            logger.debug(f"分辨率检测异常: {channel_name} - {str(e)}")
        
        return 1280, 720, "auto"

# ======================== 文本处理工具 =========================
class TextUtils:
    """优化文本处理工具"""
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """标准化文本"""
        return re.sub(r'\s+', ' ', text.strip()) if text else ""
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """验证URL有效性"""
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme in ['http', 'https', 'rtmp', 'rtsp'], result.netloc])
        except Exception:
            return False
    
    @staticmethod
    def parse_channel_line(line: str) -> Optional[Tuple[str, str]]:
        """优化频道行解析"""
        line = TextUtils.normalize_text(line)
        if not line or line.startswith('#'):
            return None
        
        # 支持多种分隔符格式
        patterns = [
            (r'^([^,]+?),\s*(https?://[^\s]+)$', '标准格式'),
            (r'^([^|]+?)\|\s*(https?://[^\s]+)$', '竖线分隔'),
            (r'#EXTINF:.*?,(.+?)\s*(?:https?://[^\s]+)?\s*(https?://[^\s]+)$', 'M3U格式'),
        ]
        
        for pattern, _ in patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                name = TextUtils.normalize_text(match.group(1))
                url = TextUtils.normalize_text(match.group(2))
                if name and url and TextUtils.is_valid_url(url):
                    return name, url
        
        return None

# ======================== 模板管理器 =========================
class TemplateManager:
    """优化模板管理器"""
    
    @staticmethod
    def load_template(file_path: str = Config.TEMPLATE_FILE) -> List[str]:
        """加载模板文件"""
        if not os.path.exists(file_path):
            Console.print_warning(f"模板文件不存在: {file_path}")
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip()]
            Console.print_success(f"模板加载成功: {len(lines)}行")
            return lines
        except Exception as e:
            Console.print_error(f"模板加载失败: {str(e)}")
            return []
    
    @staticmethod
    def parse_template_structure(lines: List[str]) -> Dict[str, List[str]]:
        """解析模板结构"""
        structure = {}
        current_category = "默认分类"
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('##'):
                continue
                
            if '#genre#' in line:
                current_category = line.split(',')[0].strip()
                structure[current_category] = []
            elif current_category and line and not line.startswith('#'):
                channel_name = line.split(',')[0].strip()
                if channel_name:
                    structure[current_category].append(channel_name)
        
        return structure

# ======================== 核心处理器 =========================
class IPTVProcessor:
    """优化IPTV处理器主类"""
    
    def __init__(self):
        self.session = self._create_optimized_session()
        self.cache_manager = CacheManager()
        self.ffmpeg_detector = FFmpegDetector()
        self.stats = ProcessingStats()
        self._stop_event = Event()
        self._health_monitor_thread = None
        
        # FFmpeg可用性检查
        if self.ffmpeg_detector.is_available():
            Console.print_success("FFmpeg检测器已启用")
        else:
            Console.print_warning("FFmpeg未找到，使用基础测速模式")
    
    def _create_optimized_session(self) -> requests.Session:
        """创建高度优化的会话"""
        session = requests.Session()
        
        # 优化连接池配置
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=50,
            pool_maxsize=100,
            max_retries=2,
            pool_block=False
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        # 设置默认超时
        def request_with_timeout(method, url, **kwargs):
            kwargs.setdefault('timeout', Config.REQUEST_TIMEOUT)
            return requests.Session.request(session, method, url, **kwargs)
        
        session.request = request_with_timeout
        return session
    
    def _start_health_monitor(self):
        """启动健康监控"""
        if not PSUTIL_AVAILABLE:
            return
            
        def monitor():
            while not self._stop_event.is_set():
                try:
                    self.stats.update_memory_peak()
                    self._stop_event.wait(5)  # 每5秒检查一次
                except Exception:
                    break
        
        self._health_monitor_thread = threading.Thread(target=monitor, daemon=True)
        self._health_monitor_thread.start()
    
    def _fetch_single_source_with_retry(self, url: str) -> Optional[str]:
        """带重试的源抓取"""
        for attempt in range(Config.MAX_RETRIES):
            try:
                # 先检查缓存
                cached_content = self.cache_manager.get_cached_source(url)
                if cached_content:
                    self.stats.cache_hits += 1
                    return cached_content
                
                # 抓取新内容
                content = self._fetch_single_source(url)
                if content:
                    # 缓存成功结果
                    self.cache_manager.cache_source(url, content)
                    return content
                    
            except Exception as e:
                if attempt == Config.MAX_RETRIES - 1:
                    logger.warning(f"源抓取失败 {url} after {Config.MAX_RETRIES} attempts: {e}")
                    return None
                
                delay = Config.RETRY_DELAY * (2 ** attempt)  # 指数退避
                logger.debug(f"第{attempt + 1}次重试 {url} in {delay}s")
                time.sleep(delay)
                self.stats.retry_attempts += 1
        
        return None
    
    def _fetch_single_source(self, url: str) -> Optional[str]:
        """优化单源抓取"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/plain,text/html,*/*',
                'Accept-Encoding': 'gzip, deflate',
            }
            
            response = self.session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            content = response.text.strip()
            return content if len(content) > Config.MIN_CONTENT_LENGTH else None
            
        except Exception as e:
            logger.debug(f"源抓取失败 {url}: {str(e)}")
            return None
    
    def _parse_channels_streaming(self, sources: List[str]) -> Generator[ChannelInfo, None, None]:
        """流式解析频道，减少内存占用"""
        seen_urls = set()
        
        for i, content in enumerate(sources, 1):
            if self._stop_event.is_set():
                break
                
            channels_from_source = 0
            for line in content.splitlines():
                if self._stop_event.is_set():
                    break
                    
                result = TextUtils.parse_channel_line(line)
                if result:
                    name, url = result
                    
                    # URL去重
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    
                    # 创建频道对象
                    channel = ChannelInfo(name=name, url=url, source=f"Source_{i}")
                    
                    # 智能分辨率检测
                    width, height, _ = ResolutionDetector.detect_from_name(name)
                    channel.width = width
                    channel.height = height
                    
                    channels_from_source += 1
                    yield channel
            
            Console.print_info(f"源{i}: 解析{channels_from_source}个频道")
    
    def _advanced_ffmpeg_test(self, channel: ChannelInfo) -> ChannelInfo:
        """使用FFmpeg进行高级流媒体测试"""
        self.stats.ffmpeg_tests += 1
        
        if not self.ffmpeg_detector.is_available():
            return channel
        
        try:
            # 第一步：快速流媒体分析
            Console.print_ffmpeg(f"分析流媒体: {channel.name}")
            probe_data = self.ffmpeg_detector.analyze_stream(channel.url)
            
            if probe_data:
                # 解析流媒体质量信息
                stream_quality = self.ffmpeg_detector.parse_stream_quality(probe_data)
                channel.stream_quality = stream_quality
                channel.ffmpeg_supported = True
                
                # 验证流媒体质量
                if (stream_quality.has_video and 
                    stream_quality.video_bitrate >= Config.MIN_VIDEO_BITRATE and
                    stream_quality.total_bitrate >= Config.MIN_VIDEO_BITRATE + Config.MIN_AUDIO_BITRATE):
                    
                    # 第二步：快速连接测试
                    quick_test = self.ffmpeg_detector.quick_test_stream(channel.url, duration=3)
                    if quick_test and quick_test.get('success'):
                        channel.status = ChannelStatus.VALID
                        channel.speed = quick_test.get('speed', 1.0)
                        self.stats.ffmpeg_success += 1
                        
                        Console.print_success(
                            f"{channel.name:<25} | "
                            f"FFmpeg✅ | "
                            f"码率:{channel.bitrate_str:>8} | "
                            f"编码:{channel.codec_str:>10} | "
                            f"分辨率:{channel.resolution_str:>9}"
                        )
                    else:
                        channel.status = ChannelStatus.FORMAT_ERROR
                        self.stats.network_errors += 1  # 修复：添加错误统计
                else:
                    channel.status = ChannelStatus.CODEC_ERROR
                    self.stats.network_errors += 1  # 修复：添加错误统计
            else:
                channel.status = ChannelStatus.UNREACHABLE
                self.stats.network_errors += 1  # 修复：添加错误统计
                
        except Exception as e:
            logger.debug(f"FFmpeg测试异常 {channel.url}: {e}")
            channel.status = ChannelStatus.UNREACHABLE
            self.stats.network_errors += 1  # 修复：添加错误统计
        
        return channel
    
    def _basic_http_test(self, channel: ChannelInfo) -> ChannelInfo:
        """基础HTTP测速 - 修复网络错误统计"""
        try:
            start_time = time.time()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Range': 'bytes=0-102399'  # 100KB
            }
            
            response = self.session.get(
                channel.url,
                headers=headers,
                timeout=Config.SPEED_TEST_TIMEOUT,
                stream=True
            )
            
            if response.status_code in [200, 206]:
                content_length = 0
                start_read = time.time()
                
                for chunk in response.iter_content(8192):
                    if self._stop_event.is_set():
                        break
                    content_length += len(chunk)
                    if content_length >= 102400:  # 100KB
                        break
                    if time.time() - start_read > Config.SPEED_TEST_TIMEOUT:
                        break
                
                total_time = time.time() - start_time
                channel.delay = total_time
                channel.speed = content_length / total_time / 1024 if total_time > 0 else 0
                
                if channel.speed >= Config.MIN_SPEED_KBPS and total_time <= Config.SPEED_TEST_TIMEOUT:
                    channel.status = ChannelStatus.VALID
                    Console.print_success(
                        f"{channel.name:<25} | "
                        f"HTTP✅ | "
                        f"延迟:{channel.delay:5.2f}s | "
                        f"速度:{channel.speed:6.1f}KB/s"
                    )
                else:
                    channel.status = ChannelStatus.LOW_SPEED
                    self.stats.network_errors += 1  # 修复：添加错误统计
            else:
                channel.status = ChannelStatus.UNREACHABLE
                self.stats.network_errors += 1  # 修复：添加错误统计
                
        except requests.exceptions.Timeout:
            channel.status = ChannelStatus.TIMEOUT
            self.stats.network_errors += 1  # 修复：添加错误统计
        except requests.exceptions.ConnectionError:
            channel.status = ChannelStatus.UNREACHABLE
            self.stats.network_errors += 1  # 修复：添加错误统计
        except Exception:
            channel.status = ChannelStatus.UNREACHABLE
            self.stats.network_errors += 1  # 修复：添加错误统计
        
        channel.last_checked = time.time()
        return channel
    
    def _hybrid_speed_test(self, channel: ChannelInfo) -> ChannelInfo:
        """混合测速策略：FFmpeg优先，HTTP备用"""
        # 首先尝试FFmpeg检测
        ffmpeg_result = self._advanced_ffmpeg_test(channel)
        
        if ffmpeg_result.is_valid:
            return ffmpeg_result
        
        # FFmpeg失败时使用HTTP测速
        return self._basic_http_test(channel)
    
    def _fuzzy_template_matching(self, channels: List[ChannelInfo]) -> List[ChannelInfo]:
        """模糊模板匹配 - 优化进度显示"""
        Console.print_info("开始模板匹配...")
        
        template_lines = TemplateManager.load_template()
        if not template_lines:
            Console.print_warning("无模板文件，返回所有有效频道")
            return channels  # 修复：无模板时返回所有频道
        
        template_structure = TemplateManager.parse_template_structure(template_lines)
        if not template_structure:
            Console.print_warning("模板解析为空，返回所有有效频道")
            return channels  # 修复：空模板时返回所有频道
        
        # 获取所有模板频道名称
        template_names = set()
        for category_channels in template_structure.values():
            template_names.update([name.lower().strip() for name in category_channels if name.strip()])
        
        Console.print_info(f"模板频道数: {len(template_names)}")
        
        # 精确匹配
        matched_channels = []
        exact_matches = 0
        
        for i, channel in enumerate(channels, 1):
            channel_name_lower = channel.name.lower().strip()
            if channel_name_lower in template_names:
                matched_channels.append(channel)
                exact_matches += 1
            
            # 优化：显示匹配进度
            if i % 50 == 0 or i == len(channels):
                Console.print_progress(i, len(channels), "模板匹配进度")
        
        Console.print_success(f"精确匹配: {exact_matches}/{len(channels)}")
        
        # 如果没有精确匹配，尝试模糊匹配
        if exact_matches == 0 and FUZZYWUZZY_AVAILABLE:
            Console.print_info("尝试模糊匹配...")
            fuzzy_matches = 0
            
            for i, channel in enumerate(channels, 1):
                if channel in matched_channels:  # 跳过已匹配的
                    continue
                    
                channel_name_lower = channel.name.lower().strip()
                best_score = 0
                
                for template_name in template_names:
                    score = fuzz.token_sort_ratio(channel_name_lower, template_name)
                    if score > Config.FUZZY_MATCH_THRESHOLD and score > best_score:
                        best_score = score
                
                if best_score >= Config.FUZZY_MATCH_THRESHOLD:
                    matched_channels.append(channel)
                    fuzzy_matches += 1
                    logger.debug(f"模糊匹配: {channel.name} -> {best_score}分")
                
                # 优化：显示模糊匹配进度
                if i % 20 == 0 or i == len(channels):
                    Console.print_progress(i, len(channels), "模糊匹配进度")
            
            Console.print_success(f"模糊匹配: {fuzzy_matches}个")
        elif exact_matches == 0:
            Console.print_warning("fuzzywuzzy 未安装，跳过模糊匹配")
        
        self.stats.template_matched = len(matched_channels)
        Console.print_success(f"模板匹配完成: {len(matched_channels)}/{len(channels)}")
        return matched_channels
    
    def health_check(self) -> Dict[str, Any]:
        """系统健康检查"""
        health_info = {
            "version": Config.VERSION,
            "running_time": self.stats.elapsed_time,
            "active_threads": threading.active_count(),
            "memory_peak_mb": self.stats.memory_peak,
            "network_errors": self.stats.network_errors,
            "cache_hits": self.stats.cache_hits,
            "retry_attempts": self.stats.retry_attempts,
            "ffmpeg_tests": self.stats.ffmpeg_tests,
            "ffmpeg_success": self.stats.ffmpeg_success,
            "ffmpeg_available": self.ffmpeg_detector.is_available(),
        }
        
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                health_info.update({
                    "memory_current_mb": process.memory_info().rss / 1024 / 1024,
                    "cpu_percent": process.cpu_percent(),
                    "disk_usage": psutil.disk_usage('.')._asdict(),
                })
            except Exception:
                pass
        
        return health_info
    
    def _generate_quality_report(self, channels: List[ChannelInfo]) -> bool:
        """生成质量报告"""
        try:
            report = {
                "generated_at": datetime.now().isoformat(),
                "total_channels": len(channels),
                "ffmpeg_tested": sum(1 for c in channels if c.ffmpeg_supported),
                "quality_stats": {
                    "uhd_8k": sum(1 for c in channels if c.quality == ResolutionQuality.UHD_8K),
                    "uhd_4k": sum(1 for c in channels if c.quality == ResolutionQuality.UHD_4K),
                    "fhd_1080p": sum(1 for c in channels if c.quality == ResolutionQuality.FHD_1080P),
                    "hd_720p": sum(1 for c in channels if c.quality == ResolutionQuality.HD_720P),
                    "sd_480p": sum(1 for c in channels if c.quality == ResolutionQuality.SD_480P),
                },
                "channels": []
            }
            
            for channel in channels:
                channel_info = {
                    "name": channel.name,
                    "url": channel.url,
                    "resolution": channel.resolution_str,
                    "bitrate": channel.bitrate_str,
                    "codec": channel.codec_str,
                    "speed": channel.speed,
                    "delay": channel.delay,
                    "ffmpeg_supported": channel.ffmpeg_supported,
                    "quality": channel.quality.name,
                    "stream_type": channel.stream_quality.stream_type.value,
                    "has_video": channel.stream_quality.has_video,
                    "has_audio": channel.stream_quality.has_audio,
                    "is_live": channel.stream_quality.is_live,
                }
                report["channels"].append(channel_info)
            
            with open(Config.OUTPUT_QUALITY_REPORT, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            Console.print_success(f"质量报告生成成功: {Config.OUTPUT_QUALITY_REPORT}")
            return True
            
        except Exception as e:
            Console.print_error(f"质量报告生成失败: {str(e)}")
            return False
    
    def process(self) -> bool:
        """优化主处理流程"""
        Console.print_success(f"{Config.APP_NAME} v{Config.VERSION} 开始处理")
        
        try:
            # 0. 启动健康监控
            self._start_health_monitor()
            
            # 1. 系统初始化
            self._initialize_system()
            
            # 2. 多源抓取
            sources_content = self._fetch_multiple_sources()
            if not sources_content:
                Console.print_error("无有效源数据")
                return False
            
            # 3. 智能解析（流式）
            all_channels = list(self._parse_channels_streaming(sources_content))
            if not all_channels:
                Console.print_error("无有效频道数据")
                return False
            
            self.stats.total_channels = len(all_channels)
            Console.print_success(f"频道解析完成: {len(all_channels)}个频道")
            
            # 4. 智能测速（FFmpeg + HTTP混合）
            valid_channels = self._speed_test_channels(all_channels)
            if not valid_channels:
                Console.print_error("无有效频道通过测速")
                return False
            
            # 5. 模板匹配
            final_channels = self._fuzzy_template_matching(valid_channels)
            if not final_channels:
                Console.print_error("无频道匹配模板")
                return False
            
            # 6. 生成纯净输出
            success = self._generate_outputs(final_channels)
            
            # 7. 生成质量报告
            self._generate_quality_report(final_channels)
            
            if success:
                self._print_final_stats()
            
            return success
            
        except KeyboardInterrupt:
            Console.print_warning("用户中断处理")
            self._stop_event.set()
            return False
        except Exception as e:
            Console.print_error(f"处理异常: {str(e)}")
            logger.exception("详细异常信息")
            return False
        finally:
            self.stats.end_time = time.time()
            self._stop_event.set()
            if hasattr(self, 'session'):
                self.session.close()
            # 清理资源
            self.cache_manager.clear_expired_cache()
            Console.print_info("资源清理完成")
    
    def _initialize_system(self):
        """系统初始化"""
        Console.print_info("系统初始化中...")
        Console.print_info(f"Python版本: {platform.python_version()}")
        Console.print_info(f"平台: {platform.system()} {platform.release()}")
        Console.print_info(f"CPU核心: {os.cpu_count()}")
        Console.print_info(f"缓存目录: {Config.CACHE_DIR}")
        Console.print_info(f"FFmpeg可用: {self.ffmpeg_detector.is_available()}")
        
        # 清理过期缓存
        self.cache_manager.clear_expired_cache()
    
    def _fetch_multiple_sources(self) -> List[str]:
        """优化多源并发抓取"""
        Console.print_info("开始多源抓取...")
        
        sources = Config.SOURCE_URLS
        sources_content = []
        self.stats.total_sources = len(sources)
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS_SOURCE) as executor:
            futures = {executor.submit(self._fetch_single_source_with_retry, url): url for url in sources}
            
            for i, future in enumerate(as_completed(futures), 1):
                url = futures[future]
                try:
                    content = future.result(timeout=30)
                    if content:
                        sources_content.append(content)
                        self.stats.valid_sources += 1
                        Console.print_success(f"[{i}/{len(sources)}] 抓取成功: {url}")
                    else:
                        Console.print_warning(f"[{i}/{len(sources)}] 抓取失败: {url}")
                except Exception as e:
                    Console.print_warning(f"[{i}/{len(sources)}] 抓取异常: {url} - {str(e)}")
                
                Console.print_progress(i, len(sources), "源抓取进度")
        
        Console.print_info(f"源抓取完成: {len(sources_content)}/{len(sources)}")
        Console.print_info(f"缓存命中: {self.stats.cache_hits}")
        return sources_content
    
    def _speed_test_channels(self, channels: List[ChannelInfo]) -> List[ChannelInfo]:
        """优化智能测速 - 修复进度显示"""
        Console.print_info("开始频道测速...")
        
        valid_channels = []
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS_SPEED_TEST) as executor:
            futures = {executor.submit(self._hybrid_speed_test, channel): channel 
                      for channel in channels}
            
            for i, future in enumerate(as_completed(futures), 1):
                if self._stop_event.is_set():
                    break
                    
                channel = futures[future]
                try:
                    tested_channel = future.result(timeout=Config.FFMPEG_TIMEOUT + 5)
                    if tested_channel.is_valid:
                        valid_channels.append(tested_channel)
                    
                    # 优化进度显示频率
                    if i % 5 == 0 or i == len(channels) or i <= 10:
                        Console.print_progress(i, len(channels), "测速进度")
                        
                except Exception as e:
                    logger.warning(f"测速异常 {channel.name}: {str(e)}")
        
        self.stats.speed_tested = len(valid_channels)
        Console.print_success(f"测速完成: {len(valid_channels)}/{len(channels)}个有效")
        Console.print_info(f"FFmpeg成功检测: {self.stats.ffmpeg_success}/{self.stats.ffmpeg_tests}")
        return valid_channels
    
    def _generate_outputs(self, channels: List[ChannelInfo]) -> bool:
        """生成纯净输出"""
        Console.print_info("生成输出文件...")
        
        try:
            # 生成TXT
            txt_success = self._generate_txt_file(channels)
            # 生成M3U
            m3u_success = self._generate_m3u_file(channels)
            
            self.stats.final_channels = len(channels)
            return txt_success and m3u_success
            
        except Exception as e:
            Console.print_error(f"生成输出失败: {str(e)}")
            return False
    
    def _generate_txt_file(self, channels: List[ChannelInfo]) -> bool:
        """生成纯净TXT文件"""
        try:
            content = self._generate_txt_content(channels)
            with open(Config.OUTPUT_TXT, 'w', encoding='utf-8') as f:
                f.write(content)
            Console.print_success(f"TXT文件生成成功: {Config.OUTPUT_TXT}")
            return True
        except Exception as e:
            Console.print_error(f"TXT文件生成失败: {str(e)}")
            return False
    
    def _generate_m3u_file(self, channels: List[ChannelInfo]) -> bool:
        """生成纯净M3U文件"""
        try:
            content = self._generate_m3u_content(channels)
            with open(Config.OUTPUT_M3U, 'w', encoding='utf-8') as f:
                f.write(content)
            Console.print_success(f"M3U文件生成成功: {Config.OUTPUT_M3U}")
            return True
        except Exception as e:
            Console.print_error(f"M3U文件生成失败: {str(e)}")
            return False
    
    def _generate_txt_content(self, channels: List[ChannelInfo]) -> str:
        """生成纯净TXT内容"""
        template = TemplateManager.load_template()
        structure = TemplateManager.parse_template_structure(template) if template else {"默认分类": [c.name for c in channels]}
        
        lines = [
            f"# IPTV频道列表 - {Config.APP_NAME} v{Config.VERSION}",
            f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"# 总频道数: {len(channels)}",
            f"# 纯净输出 - 无速度/分辨率标识",
            ""
        ]
        
        for category, names in structure.items():
            lines.append(f"{category},#genre#")
            
            category_channels = [c for c in channels if c.name.lower() in [n.lower() for n in names]]
            # 按速度排序
            category_channels.sort(key=lambda x: x.speed, reverse=True)
            
            for channel in category_channels:
                lines.append(f"{channel.name},{channel.url}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_m3u_content(self, channels: List[ChannelInfo]) -> str:
        """生成纯净M3U内容"""
        template = TemplateManager.load_template()
        structure = TemplateManager.parse_template_structure(template) if template else {"默认分类": [c.name for c in channels]}
        
        lines = ["#EXTM3U"]
        
        for category, names in structure.items():
            category_channels = [c for c in channels if c.name.lower() in [n.lower() for n in names]]
            # 按速度排序
            category_channels.sort(key=lambda x: x.speed, reverse=True)
            
            for channel in category_channels:
                lines.extend([
                    f'#EXTINF:-1 group-title="{category}",{channel.name}',
                    channel.url
                ])
        
        return "\n".join(lines)
    
    def _print_final_stats(self):
        """打印最终统计"""
        Console.print_success("处理完成！")
        Console.print_info(f"处理耗时: {self.stats.elapsed_time:.2f}秒")
        Console.print_info(f"有效源: {self.stats.valid_sources}/{self.stats.total_sources}")
        Console.print_info(f"总频道: {self.stats.total_channels}")
        Console.print_info(f"测速有效: {self.stats.speed_tested}")
        Console.print_info(f"模板匹配: {self.stats.template_matched}")
        Console.print_info(f"最终输出: {self.stats.final_channels}")
        Console.print_info(f"缓存命中: {self.stats.cache_hits}")
        Console.print_info(f"重试次数: {self.stats.retry_attempts}")
        Console.print_info(f"FFmpeg测试: {self.stats.ffmpeg_tests}")
        Console.print_info(f"FFmpeg成功: {self.stats.ffmpeg_success}")
        
        if self.stats.memory_peak > 0:
            Console.print_info(f"内存峰值: {self.stats.memory_peak:.1f}MB")
        
        if self.stats.network_errors > 0:
            Console.print_warning(f"网络错误: {self.stats.network_errors}")
        
        # 打印健康状态
        health = self.health_check()
        Console.print_info("系统健康状态:")
        for key, value in health.items():
            if key not in ['running_time', 'memory_peak_mb']:  # 这些已经显示过了
                Console.print_info(f"  {key}: {value}")

# ======================== 依赖检查 =========================
def check_dependencies():
    """检查依赖"""
    print("正在检查依赖...")
    
    dependencies = {
        'requests': '网络请求',
        'psutil': '系统监控',
        'fuzzywuzzy': '模糊匹配',
        'colorama': 'Windows颜色支持',
    }
    
    missing = []
    for package, description in dependencies.items():
        try:
            if package == 'fuzzywuzzy':
                __import__('fuzzywuzzy.fuzz')
            else:
                __import__(package)
            print(f"✅ {package} - {description}")
        except ImportError:
            print(f"❌ {package} - {description}")
            missing.append(package)
    
    # 检查FFmpeg
    detector = FFmpegDetector()
    if detector.is_available():
        print("✅ FFmpeg - 流媒体分析")
    else:
        print("❌ FFmpeg - 流媒体分析 (未找到)")
        missing.append('ffmpeg')
    
    if missing:
        print(f"\n缺少依赖: {', '.join(missing)}")
        print("安装命令: pip install " + " ".join([p for p in missing if p != 'ffmpeg']))
        if 'ffmpeg' in missing:
            print("FFmpeg 需要手动安装:")
            print("  Ubuntu: sudo apt install ffmpeg")
            print("  macOS: brew install ffmpeg")
            print("  Windows: 下载 https://ffmpeg.org/download.html")
        return False
    else:
        print("\n✅ 所有依赖已安装")
        return True

# ======================== 主程序 =========================
def main():
    """程序入口"""
    try:
        # 显示启动信息
        Console.print_success(f"{Config.APP_NAME} v{Config.VERSION}")
        Console.print_info("正在初始化系统...")
        
        # 检查依赖（可选）
        if len(sys.argv) > 1 and sys.argv[1] == '--check-deps':
            if not check_dependencies():
                return 1
            return 0
        
        # 创建处理器实例
        processor = IPTVProcessor()
        
        # 显示系统信息
        health = processor.health_check()
        Console.print_info(f"FFmpeg可用: {health.get('ffmpeg_available', False)}")
        
        # 开始处理
        Console.print_info("开始处理IPTV源...")
        success = processor.process()
        
        if success:
            Console.print_success("IPTV处理完成！")
            Console.print_info(f"输出文件:")
            Console.print_info(f"  - {Config.OUTPUT_TXT} (TXT格式)")
            Console.print_info(f"  - {Config.OUTPUT_M3U} (M3U格式)")
            Console.print_info(f"  - {Config.OUTPUT_QUALITY_REPORT} (质量报告)")
        else:
            Console.print_error("处理失败，请检查日志文件了解详情")
            
        return 0 if success else 1
        
    except KeyboardInterrupt:
        Console.print_warning("用户中断程序执行")
        return 1
    except Exception as e:
        Console.print_error(f"程序异常: {str(e)}")
        logger.exception("程序异常详情:")
        return 1

if __name__ == "__main__":
    # 检查依赖参数
    if len(sys.argv) > 1 and sys.argv[1] == '--check-deps':
        sys.exit(0 if check_dependencies() else 1)
    else:
        sys.exit(main())
