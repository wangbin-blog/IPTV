#!/usr/bin/env python3
"""
IPTV源处理工具 - 优化版
功能：多源抓取、测速筛选、分辨率过滤、严格模板匹配
作者：优化版
版本：2.0
"""

import requests
import re
import os
import time
import logging
import json
import stat
import platform
import random
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Dict, Tuple, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import sys

# ======================== 数据类型定义 =========================
class ResolutionQuality(Enum):
    """分辨率质量等级"""
    UHD_4K = "4K"
    FHD_1080P = "1080p"
    HD_720P = "720p"
    SD_480P = "480p"
    LOW_360P = "360p"
    UNKNOWN = "unknown"
    LOW_QUALITY = "low"

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

@dataclass
class CategoryInfo:
    """分类信息数据类"""
    name: str
    channels: List[str]
    marker: str

@dataclass
class TemplateStructure:
    """模板结构数据类"""
    type: str  # 'category' or 'channel'
    name: str
    category: Optional[str] = None
    line_num: int = 0

# ======================== 配置管理类 =========================
class Config:
    """配置管理类"""
    
    # 基础功能配置
    SOURCE_URLS = [
        "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
        "https://live.zbds.top/tv/iptv6.txt",
        "https://live.zbds.top/tv/iptv4.txt",
    ]
    
    # 文件配置
    DEFAULT_TEMPLATE = "demo.txt"
    BACKUP_TEMPLATE = "demo_backup.txt"
    TXT_OUTPUT = "iptv.txt"
    M3U_OUTPUT = "iptv.m3u"
    CACHE_FILE = ".iptv_valid_cache.json"
    
    # 性能配置
    MAX_INTERFACES_PER_CHANNEL = 5
    SPEED_TEST_TIMEOUT = 8
    MAX_SPEED_TEST_WORKERS = 15
    MAX_FETCH_WORKERS = 5
    MAX_RESOLUTION_WORKERS = 8
    
    # 缓存配置
    CACHE_EXPIRE = 3600
    MAX_CACHE_SIZE = 100
    
    # 网络配置
    MAX_REDIRECTS = 3
    REQ_INTERVAL = [0.2, 0.3, 0.4, 0.5]
    MIN_CONTENT_LEN = 100
    TEST_URL = "https://www.baidu.com"
    
    # 模板配置
    CATEGORY_MARKER = "#genre#"
    
    # 分辨率过滤配置
    RESOLUTION_FILTER = {
        "enable": True,
        "min_width": 1280,
        "min_height": 720,
        "strict_mode": True,
        "remove_low_resolution": True,
        "low_res_threshold": (854, 480),
        "preferred_resolutions": ["4K", "1080p", "720p"],
        "timeout": 10,
        "keep_unknown": False,
    }
    
    @classmethod
    def validate(cls) -> bool:
        """验证配置完整性"""
        validators = [
            (bool(cls.SOURCE_URLS), "SOURCE_URLS 不能为空"),
            (cls.MAX_FETCH_WORKERS > 0, "MAX_FETCH_WORKERS 必须大于0"),
            (cls.MAX_SPEED_TEST_WORKERS > 0, "MAX_SPEED_TEST_WORKERS 必须大于0"),
            (cls.SPEED_TEST_TIMEOUT > 0, "SPEED_TEST_TIMEOUT 必须大于0"),
            (bool(cls.REQ_INTERVAL), "REQ_INTERVAL 不能为空"),
        ]
        
        if cls.RESOLUTION_FILTER["enable"]:
            resolution_validators = [
                (cls.RESOLUTION_FILTER["min_width"] > 0 and cls.RESOLUTION_FILTER["min_height"] > 0, 
                 "分辨率最小宽度和高度必须大于0"),
                (cls.RESOLUTION_FILTER["timeout"] > 0, "分辨率检测超时必须大于0"),
                (cls.RESOLUTION_FILTER["max_resolution_workers"] > 0, 
                 "分辨率检测并发线程数必须大于0"),
            ]
            validators.extend(resolution_validators)
        
        errors = [msg for condition, msg in validators if not condition]
        
        if errors:
            error_msg = "配置验证失败:\n" + "\n".join(f"  - {error}" for error in errors)
            Console.print_error(error_msg)
            return False
        
        return True

# ======================== 工具类 =========================
class Console:
    """控制台输出工具类"""
    
    # 颜色代码
    COLORS = {
        'green': '\033[92m',
        'red': '\033[91m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'cyan': '\033[96m',
        'magenta': '\033[95m',
        'reset': '\033[0m'
    }
    
    # 线程安全锁
    print_lock = Lock()
    
    @classmethod
    def _init_colors(cls):
        """初始化颜色支持"""
        if platform.system() == "Windows":
            try:
                import colorama
                colorama.init()
                # 在Windows上使用colorama的颜色
                cls.COLORS = {k: getattr(colorama.Fore, v.upper()) 
                            for k, v in cls.COLORS.items()}
            except ImportError:
                # 没有colorama，在Windows上不使用颜色
                cls.COLORS = {k: '' for k in cls.COLORS}
    
    @classmethod
    def print(cls, message: str, color: str = None, icon: str = ""):
        """线程安全的彩色输出"""
        with cls.print_lock:
            color_code = cls.COLORS.get(color, '')
            reset_code = cls.COLORS['reset']
            formatted_msg = f"{icon} {message}" if icon else message
            if color_code:
                print(f"{color_code}{formatted_msg}{reset_code}")
            else:
                print(formatted_msg)
    
    @classmethod
    def print_success(cls, message: str):
        """成功信息"""
        cls.print(message, 'green', '✅')
    
    @classmethod
    def print_error(cls, message: str):
        """错误信息"""
        cls.print(message, 'red', '❌')
    
    @classmethod
    def print_warning(cls, message: str):
        """警告信息"""
        cls.print(message, 'yellow', '⚠️')
    
    @classmethod
    def print_info(cls, message: str):
        """信息提示"""
        cls.print(message, 'blue', '🔍')
    
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

class FileUtils:
    """文件工具类"""
    
    @staticmethod
    def set_permissions(file_path: str) -> bool:
        """设置文件权限（Linux/Mac）"""
        if platform.system() == "Windows":
            return True
        
        try:
            os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
            return True
        except Exception as e:
            Console.print_warning(f"文件权限设置失败：{str(e)}")
            return False
    
    @staticmethod
    def ensure_directory(file_path: str) -> bool:
        """确保文件所在目录存在"""
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                return True
            except Exception as e:
                Console.print_error(f"创建目录失败：{str(e)}")
                return False
        return True
    
    @staticmethod
    def read_file_lines(file_path: str) -> List[str]:
        """读取文件所有行"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f.readlines()]
        except Exception as e:
            Console.print_error(f"读取文件失败 {file_path}: {str(e)}")
            return []
    
    @staticmethod
    def write_file(file_path: str, content: str) -> bool:
        """写入文件"""
        try:
            FileUtils.ensure_directory(file_path)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            FileUtils.set_permissions(file_path)
            return True
        except Exception as e:
            Console.print_error(f"写入文件失败 {file_path}: {str(e)}")
            return False

class NetworkUtils:
    """网络工具类"""
    
    @staticmethod
    def check_connectivity() -> bool:
        """检查网络连接"""
        Console.print_info("正在检测网络连接...")
        try:
            timeout = 5 if platform.system() == "Windows" else 3
            response = requests.get(Config.TEST_URL, timeout=timeout)
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
    
    # 正则表达式预编译
    SPACE_PATTERN = re.compile(r'^\s+|\s+$|\s+(?=\s)')
    CHANNEL_PATTERN = re.compile(r'([^,]+),(https?://.+)$')
    URL_PATTERN = re.compile(r'^https?://')
    
    # 分辨率相关正则
    RESOLUTION_PATTERN = re.compile(r'(\d{3,4})[×xX*](\d{3,4})')
    RESOLUTION_NAME_PATTERN = re.compile(r'(4K|UHD|1080[Pp]|720[Pp]|480[Pp]|360[Pp]|SD|HD|FHD|超清|高清|标清)')
    LOW_RES_INDICATORS = re.compile(r'(标清|流畅|流畅版|低速|低码|480|360|SD|low)', re.IGNORECASE)
    
    @staticmethod
    def clean_text(text: str) -> str:
        """清理文本中的多余空格"""
        if not text:
            return ""
        return TextUtils.SPACE_PATTERN.sub("", str(text).strip())
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """验证URL格式"""
        return bool(url and TextUtils.URL_PATTERN.match(url))
    
    @staticmethod
    def parse_channel_line(line: str) -> Optional[Tuple[str, str]]:
        """解析频道行"""
        match = TextUtils.CHANNEL_PATTERN.match(line.strip())
        if match:
            name, url = match.groups()
            name = TextUtils.clean_text(name)
            url = TextUtils.clean_text(url)
            if name and url and TextUtils.is_valid_url(url):
                return name, url
        return None
    
    @staticmethod
    def normalize_channel_name(name: str) -> str:
        """标准化频道名用于匹配"""
        return name.lower().replace(' ', '').replace('高清', '').replace('标清', '')
    
    @staticmethod
    def parse_resolution(channel_name: str) -> Tuple[Optional[int], Optional[int], str, ResolutionQuality]:
        """从频道名解析分辨率信息"""
        if not channel_name:
            return None, None, "unknown", ResolutionQuality.UNKNOWN
        
        quality = ResolutionQuality.UNKNOWN
        
        # 检测低分辨率标识
        if TextUtils.LOW_RES_INDICATORS.search(channel_name):
            quality = ResolutionQuality.LOW_QUALITY
        
        # 匹配数字分辨率格式
        resolution_match = TextUtils.RESOLUTION_PATTERN.search(channel_name)
        if resolution_match:
            width = int(resolution_match.group(1))
            height = int(resolution_match.group(2))
            res_name = f"{width}x{height}"
            
            # 根据分辨率判断质量
            if width >= 3840 or height >= 2160:
                quality = ResolutionQuality.UHD_4K
            elif width >= 1920 or height >= 1080:
                quality = ResolutionQuality.FHD_1080P
            elif width >= 1280 or height >= 720:
                quality = ResolutionQuality.HD_720P
            elif width < 1280 or height < 720:
                quality = ResolutionQuality.LOW_QUALITY
                
            return width, height, res_name, quality
        
        # 匹配标准分辨率名称
        name_match = TextUtils.RESOLUTION_NAME_PATTERN.search(channel_name)
        if name_match:
            res_name = name_match.group(1).upper()
            resolution_map = {
                "4K": (3840, 2160, ResolutionQuality.UHD_4K),
                "UHD": (3840, 2160, ResolutionQuality.UHD_4K),
                "FHD": (1920, 1080, ResolutionQuality.FHD_1080P),
                "1080P": (1920, 1080, ResolutionQuality.FHD_1080P),
                "1080p": (1920, 1080, ResolutionQuality.FHD_1080P),
                "HD": (1280, 720, ResolutionQuality.HD_720P),
                "720P": (1280, 720, ResolutionQuality.HD_720P),
                "720p": (1280, 720, ResolutionQuality.HD_720P),
                "480P": (854, 480, ResolutionQuality.SD_480P),
                "480p": (854, 480, ResolutionQuality.SD_480P),
                "360P": (640, 360, ResolutionQuality.LOW_360P),
                "360p": (640, 360, ResolutionQuality.LOW_360P),
                "超清": (1920, 1080, ResolutionQuality.FHD_1080P),
                "高清": (1280, 720, ResolutionQuality.HD_720P),
                "标清": (854, 480, ResolutionQuality.SD_480P)
            }
            if res_name in resolution_map:
                width, height, quality = resolution_map[res_name]
                return width, height, res_name, quality
        
        return None, None, "unknown", quality
    
    @staticmethod
    def get_resolution_priority(resolution_name: str) -> int:
        """获取分辨率优先级"""
        priority_map = {
            "4K": 1, "UHD": 1,
            "1080P": 2, "1080p": 2, "FHD": 2,
            "720P": 3, "720p": 3, "HD": 3,
            "480P": 4, "480p": 4,
            "360P": 5, "360p": 5,
            "SD": 6, "标清": 6
        }
        return priority_map.get(resolution_name, 999)

# ======================== 核心功能类 =========================
class CacheManager:
    """缓存管理器"""
    
    def __init__(self):
        self.cache_file = Config.CACHE_FILE
        self.lock = Lock()
        self.cache = self._load_cache()
    
    def _load_cache(self) -> Dict[str, Any]:
        """加载缓存"""
        with self.lock:
            if not os.path.exists(self.cache_file):
                return {}
            
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                
                # 清理过期缓存
                current_time = time.time()
                valid_cache = {
                    url: info for url, info in cache.items()
                    if current_time - info.get("timestamp", 0) < Config.CACHE_EXPIRE
                }
                
                # 控制缓存大小
                if len(valid_cache) > Config.MAX_CACHE_SIZE:
                    sorted_cache = sorted(valid_cache.items(), 
                                        key=lambda x: x[1].get("timestamp", 0), 
                                        reverse=True)
                    valid_cache = dict(sorted_cache[:Config.MAX_CACHE_SIZE])
                    Console.print_warning(f"缓存超量，保留最新{Config.MAX_CACHE_SIZE}个")
                
                return valid_cache
            except Exception as e:
                Console.print_warning(f"加载缓存失败：{str(e)}，使用空缓存")
                return {}
    
    def save_cache(self) -> bool:
        """保存缓存"""
        with self.lock:
            if len(self.cache) > Config.MAX_CACHE_SIZE:
                sorted_cache = sorted(self.cache.items(), 
                                    key=lambda x: x[1].get("timestamp", 0), 
                                    reverse=True)
                self.cache = dict(sorted_cache[:Config.MAX_CACHE_SIZE])
            
            try:
                FileUtils.write_file(self.cache_file, json.dumps(self.cache, ensure_ascii=False, indent=2))
                return True
            except Exception as e:
                Console.print_warning(f"保存缓存失败：{str(e)}")
                return False
    
    def get(self, url: str) -> Optional[str]:
        """获取缓存内容"""
        with self.lock:
            if url in self.cache:
                cache_info = self.cache[url]
                if time.time() - cache_info.get("timestamp", 0) < Config.CACHE_EXPIRE:
                    if cache_info.get("valid", False):
                        Console.print_info(f"缓存命中[有效]：{url[:50]}{'...' if len(url)>50 else ''}")
                        return cache_info.get("content", "")
                    else:
                        Console.print_info(f"缓存命中[无效]：{url[:50]}{'...' if len(url)>50 else ''}（跳过）")
            return None
    
    def set(self, url: str, content: str, valid: bool = True):
        """设置缓存"""
        with self.lock:
            self.cache[url] = {
                "content": content,
                "timestamp": time.time(),
                "valid": valid
            }

class TemplateManager:
    """模板管理器"""
    
    @staticmethod
    def generate_default_template() -> bool:
        """生成默认模板"""
        default_categories = [
            CategoryInfo("央视频道", ["CCTV1", "CCTV2", "CCTV3", "CCTV5", "CCTV6", "CCTV8", "CCTV13", "CCTV14", "CCTV15"], f"央视频道,{Config.CATEGORY_MARKER}"),
            CategoryInfo("卫视频道", ["湖南卫视", "浙江卫视", "东方卫视", "江苏卫视", "北京卫视", "安徽卫视", "深圳卫视", "山东卫视"], f"卫视频道,{Config.CATEGORY_MARKER}"),
            CategoryInfo("地方频道", ["广东卫视", "四川卫视", "湖北卫视", "河南卫视", "河北卫视", "辽宁卫视", "黑龙江卫视"], f"地方频道,{Config.CATEGORY_MARKER}"),
            CategoryInfo("高清频道", ["CCTV1高清", "CCTV5高清", "湖南卫视高清", "浙江卫视高清"], f"高清频道,{Config.CATEGORY_MARKER}"),
        ]
        
        template_content = [
            f"# IPTV分类模板（自动生成于 {time.strftime('%Y-%m-%d %H:%M:%S')}）",
            f"# 系统：{platform.system()} | 格式说明：分类行（分类名,{Config.CATEGORY_MARKER}）、频道行（纯频道名）",
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
            success = FileUtils.write_file(Config.DEFAULT_TEMPLATE, "\n".join(template_content))
            if success:
                Console.print_success(f"默认模板生成成功：{os.path.abspath(Config.DEFAULT_TEMPLATE)}")
            return success
        except Exception as e:
            Console.print_error(f"生成默认模板失败：{str(e)}")
            return False
    
    @staticmethod
    def read_template_strict() -> Tuple[Optional[List[CategoryInfo]], Optional[List[str]], Optional[List[TemplateStructure]]]:
        """严格读取模板"""
        if not os.path.exists(Config.DEFAULT_TEMPLATE):
            Console.print_warning("分类模板不存在，自动生成...")
            if not TemplateManager.generate_default_template():
                return None, None, None
        
        # 备份模板
        try:
            lines = FileUtils.read_file_lines(Config.DEFAULT_TEMPLATE)
            FileUtils.write_file(Config.BACKUP_TEMPLATE, "\n".join([
                f"# 模板备份（{time.strftime('%Y-%m-%d %H:%M:%S')}）",
                f"# 源路径：{os.path.abspath(Config.DEFAULT_TEMPLATE)}",
                *lines
            ]))
        except Exception as e:
            Console.print_warning(f"模板备份失败：{str(e)}（不影响主流程）")
        
        categories = []
        current_category = None
        all_channels = []
        template_structure = []
        
        try:
            for line_num, line in enumerate(FileUtils.read_file_lines(Config.DEFAULT_TEMPLATE), 1):
                if not line or (line.startswith("#") and Config.CATEGORY_MARKER not in line):
                    continue
                
                # 处理分类行
                if Config.CATEGORY_MARKER in line:
                    parts = [p.strip() for p in line.split(Config.CATEGORY_MARKER) if p.strip()]
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
                        categories.append(CategoryInfo(cat_name, [], f"{cat_name},{Config.CATEGORY_MARKER}"))
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
        Console.print_info("注意：只保留模板内明确列出的频道，不包含其他任何频道")
        
        Console.print("  " + "-" * 60)
        for idx, cat in enumerate(categories, 1):
            Console.print(f"  {idx:2d}. {cat.name:<20} 频道数：{len(cat.channels):2d}")
        Console.print("  " + "-" * 60)
        
        return categories, all_channels, template_structure

class SourceFetcher:
    """源数据抓取器"""
    
    def __init__(self):
        self.cache_manager = CacheManager()
        self.session = NetworkUtils.create_session()
    
    def fetch_single_source(self, url: str) -> Optional[str]:
        """抓取单个源"""
        # 检查缓存
        cached_content = self.cache_manager.get(url)
        if cached_content is not None:
            return cached_content
        
        Console.print_info(f"开始抓取：{url[:50]}{'...' if len(url)>50 else ''}")
        
        try:
            # 适配多系统超时
            connect_timeout = 8 if platform.system() == "Windows" else 5
            read_timeout = 15 if platform.system() == "Windows" else 10
            
            response = self.session.get(
                url, 
                timeout=(connect_timeout, read_timeout),
                allow_redirects=True
            )
            
            if response.status_code == 200:
                content = response.text.strip()
                if len(content) >= Config.MIN_CONTENT_LEN:
                    self.cache_manager.set(url, content, True)
                    Console.print_success(f"抓取成功：{url[:50]}{'...' if len(url)>50 else ''}")
                    return content
                else:
                    Console.print_warning(f"内容过短：{url[:50]}{'...' if len(url)>50 else ''}（{len(content)}字符）")
            else:
                Console.print_warning(f"HTTP错误 {response.status_code}：{url[:50]}{'...' if len(url)>50 else ''}")
                
        except Exception as e:
            Console.print_error(f"抓取失败：{url[:50]}{'...' if len(url)>50 else ''} - {str(e)}")
        
        self.cache_manager.set(url, "", False)
        return None
    
    def fetch_all_sources(self) -> List[str]:
        """并发抓取所有源"""
        sources_content = []
        
        with ThreadPoolExecutor(max_workers=Config.MAX_FETCH_WORKERS) as executor:
            future_to_url = {
                executor.submit(self.fetch_single_source, url): url 
                for url in Config.SOURCE_URLS
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    content = future.result()
                    if content:
                        sources_content.append(content)
                except Exception as e:
                    Console.print_error(f"抓取异常：{url} - {str(e)}")
                
                # 请求间隔
                time.sleep(random.choice(Config.REQ_INTERVAL))
        
        # 保存缓存
        self.cache_manager.save_cache()
        return sources_content

class ChannelProcessor:
    """频道处理器"""
    
    @staticmethod
    def parse_channels(content: str) -> List[Tuple[str, str]]:
        """从内容解析频道列表"""
        channels = []
        for line in content.splitlines():
            result = TextUtils.parse_channel_line(line)
            if result:
                channels.append(result)
        return channels
    
    @staticmethod
    def speed_test_single(channel_data: Tuple[str, str]) -> ChannelInfo:
        """单频道测速"""
        name, url = channel_data
        if not TextUtils.is_valid_url(url):
            return ChannelInfo(name, url, float('inf'), 0.0)
        
        try:
            start_time = time.time()
            response = requests.get(
                url, 
                timeout=Config.SPEED_TEST_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0"},
                stream=True
            )
            
            if response.status_code == 200:
                # 读取前10KB计算速度
                content = b""
                for chunk in response.iter_content(chunk_size=1024):
                    content += chunk
                    if len(content) >= 10240:  # 10KB
                        break
                elapsed = time.time() - start_time
                speed = len(content) / elapsed / 1024 if elapsed > 0 else 0  # KB/s
                return ChannelInfo(name, url, elapsed, speed)
        except Exception:
            pass  # 测速失败是正常情况
        
        return ChannelInfo(name, url, float('inf'), 0.0)
    
    @staticmethod
    def speed_test_channels(channels: List[Tuple[str, str]]) -> List[ChannelInfo]:
        """并发测速频道"""
        Console.print_info(f"开始测速（{len(channels)}个频道，{Config.MAX_SPEED_TEST_WORKERS}线程）...")
        
        valid_channels = []
        with ThreadPoolExecutor(max_workers=Config.MAX_SPEED_TEST_WORKERS) as executor:
            future_to_channel = {
                executor.submit(ChannelProcessor.speed_test_single, channel): channel 
                for channel in channels
            }
            
            for future in as_completed(future_to_channel):
                channel_info = future.result()
                if channel_info.delay < float('inf'):
                    valid_channels.append(channel_info)
                    Console.print_success(f"{channel_info.name:<15} | 延迟: {channel_info.delay:.2f}s | 速度: {channel_info.speed:.1f} KB/s")
                else:
                    Console.print_error(f"{channel_info.name:<15} | 测速失败")
        
        # 按延迟排序
        valid_channels.sort(key=lambda x: x.delay)
        Console.print_success(f"测速完成 | 有效频道: {len(valid_channels)}/{len(channels)}")
        return valid_channels

class ResolutionFilter:
    """分辨率过滤器"""
    
    @staticmethod
    def detect_stream_resolution(channel_info: ChannelInfo) -> ChannelInfo:
        """检测流媒体分辨率"""
        width, height, res_name, quality = TextUtils.parse_resolution(channel_info.name)
        
        # 如果从名称中已经解析到分辨率信息
        if width and height:
            channel_info.width = width
            channel_info.height = height
            channel_info.resolution = res_name
            channel_info.quality = quality
            return channel_info
        
        # 尝试通过HTTP请求获取分辨率信息
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Range": "bytes=0-50000"
            }
            
            response = requests.get(
                channel_info.url,
                headers=headers,
                timeout=Config.RESOLUTION_FILTER["timeout"],
                stream=True
            )
            
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '').lower()
                if 'video' in content_type or any(ext in channel_info.url.lower() for ext in ['.m3u8', '.ts', '.flv', '.mp4']):
                    # 这里可以扩展为实际解析视频流信息
                    pass
                    
        except Exception:
            pass  # 分辨率检测失败是正常情况
        
        return channel_info
    
    @staticmethod
    def is_low_resolution(channel_info: ChannelInfo) -> bool:
        """判断是否为低分辨率"""
        low_width, low_height = Config.RESOLUTION_FILTER["low_res_threshold"]
        
        # 明确标记为低质量的
        if channel_info.quality == ResolutionQuality.LOW_QUALITY:
            return True
        
        # 分辨率低于阈值的
        if channel_info.width > 0 and channel_info.height > 0:
            if channel_info.width < low_width and channel_info.height < low_height:
                return True
        
        return False
    
    @staticmethod
    def filter_by_resolution(channels: List[ChannelInfo]) -> List[ChannelInfo]:
        """根据分辨率过滤频道"""
        if not Config.RESOLUTION_FILTER["enable"]:
            Console.print_warning("分辨率过滤未启用，跳过过滤")
            return channels
        
        Console.print_info(f"开始严格分辨率过滤（{len(channels)}个频道）...")
        Console.print_info(f"过滤标准：≥{Config.RESOLUTION_FILTER['min_width']}x{Config.RESOLUTION_FILTER['min_height']} | 移除低分辨率：{Config.RESOLUTION_FILTER['remove_low_resolution']}")
        
        min_width = Config.RESOLUTION_FILTER["min_width"]
        min_height = Config.RESOLUTION_FILTER["min_height"]
        filtered_channels = []
        
        with ThreadPoolExecutor(max_workers=Config.RESOLUTION_FILTER["max_resolution_workers"]) as executor:
            future_to_channel = {
                executor.submit(ResolutionFilter.detect_stream_resolution, channel): channel 
                for channel in channels
            }
            
            stats = {"high_res": 0, "low_res": 0, "unknown": 0, "removed_low": 0}
            
            for future in as_completed(future_to_channel):
                channel_info = future.result()
                
                should_keep = False
                status_color = 'red'
                status = "过滤"
                
                # 高分辨率
                if channel_info.width >= min_width and channel_info.height >= min_height:
                    should_keep = True
                    stats["high_res"] += 1
                    status_color = 'green'
                    status = "高清"
                
                # 分辨率未知
                elif channel_info.width == 0 and channel_info.height == 0:
                    if Config.RESOLUTION_FILTER["keep_unknown"] and not Config.RESOLUTION_FILTER["strict_mode"]:
                        should_keep = True
                        stats["unknown"] += 1
                        status_color = 'yellow'
                        status = "未知(保留)"
                    else:
                        stats["unknown"] += 1
                        status_color = 'red'
                        status = "未知(过滤)"
                
                # 低分辨率
                elif ResolutionFilter.is_low_resolution(channel_info):
                    if Config.RESOLUTION_FILTER["remove_low_resolution"]:
                        stats["low_res"] += 1
                        stats["removed_low"] += 1
                        status_color = 'red'
                        status = "低清(过滤)"
                    else:
                        should_keep = True
                        stats["low_res"] += 1
                        status_color = 'yellow'
                        status = "低清(保留)"
                
                # 中等分辨率但未达到最低标准
                elif channel_info.width > 0 and channel_info.height > 0:
                    if not Config.RESOLUTION_FILTER["strict_mode"]:
                        should_keep = True
                        stats["low_res"] += 1
                        status_color = 'yellow'
                        status = "标清"
                    else:
                        stats["low_res"] += 1
                        status_color = 'red'
                        status = "标清(过滤)"
                
                if should_keep:
                    filtered_channels.append(channel_info)
                
                res_display = f"{channel_info.width}x{channel_info.height}" if channel_info.width and channel_info.height else "未知"
                Console.print(f"📺 {channel_info.name:<20} | 分辨率: {res_display:<10} | 质量: {channel_info.resolution:<8} | 状态: {status}", status_color)
        
        # 按分辨率优先级排序
        filtered_channels.sort(key=lambda x: (
            TextUtils.get_resolution_priority(x.resolution) if x.resolution != "unknown" else 999,
            x.delay
        ))
        
        # 输出统计
        Console.print_info("分辨率过滤统计：")
        Console.print(f"  ├─ 高清保留：{stats['high_res']} (≥{min_width}x{min_height})", 'green')
        Console.print(f"  ├─ 标清保留：{stats['low_res'] - stats['removed_low']}", 'yellow')
        Console.print(f"  ├─ 未知保留：{stats['unknown']}", 'yellow')
        Console.print(f"  ├─ 低清过滤：{stats['removed_low']}", 'red')
        Console.print(f"  └─ 总计过滤：{len(channels) - len(filtered_channels)}/{len(channels)}", 'red')
        
        Console.print_success(f"严格分辨率过滤完成 | 最终保留: {len(filtered_channels)}/{len(channels)} 个频道")
        return filtered_channels

class TemplateMatcher:
    """模板匹配器"""
    
    @staticmethod
    def filter_channels_by_template(valid_channels: List[ChannelInfo], 
                                  template_channels: List[str],
                                  template_structure: List[TemplateStructure]) -> List[ChannelInfo]:
        """严格按模板过滤频道"""
        Console.print_info("开始按模板严格过滤频道...")
        
        # 创建频道名称映射
        template_channel_map = {}
        for template_channel in template_channels:
            normalized_name = TextUtils.normalize_channel_name(template_channel)
            template_channel_map[normalized_name] = template_channel
        
        # 过滤和匹配频道
        filtered_channels = []
        matched_count = 0
        unmatched_count = 0
        
        for template_item in template_structure:
            if template_item.type == "channel":
                template_channel_name = template_item.name
                
                # 查找匹配的源频道
                matched_source_channels = []
                for source_channel in valid_channels:
                    source_name = source_channel.name
                    
                    # 直接名称匹配
                    if template_channel_name in source_name or source_name in template_channel_name:
                        matched_source_channels.append(source_channel)
                        continue
                    
                    # 标准化匹配
                    normalized_source = TextUtils.normalize_channel_name(source_name)
                    normalized_template = TextUtils.normalize_channel_name(template_channel_name)
                    
                    if normalized_template in normalized_source or normalized_source in normalized_template:
                        matched_source_channels.append(source_channel)
                        continue
                
                if matched_source_channels:
                    # 选择最佳匹配（按延迟排序）
                    matched_source_channels.sort(key=lambda x: x.delay)
                    best_channel = matched_source_channels[0]
                    # 使用模板中的频道名
                    best_channel.name = template_channel_name
                    filtered_channels.append(best_channel)
                    matched_count += 1
                    Console.print_success(f"模板匹配: {template_channel_name} -> {matched_source_channels[0].name}")
                else:
                    unmatched_count += 1
                    Console.print_warning(f"未找到匹配: {template_channel_name}")
        
        Console.print_info("模板匹配统计：")
        Console.print(f"  ├─ 成功匹配：{matched_count}/{len([x for x in template_structure if x.type == 'channel'])}", 'green')
        Console.print(f"  ├─ 未找到匹配：{unmatched_count}", 'yellow')
        Console.print(f"  └─ 最终保留：{len(filtered_channels)} 个频道", 'green')
        
        return filtered_channels
    
    @staticmethod
    def categorize_channels_strict(valid_channels: List[ChannelInfo],
                                 template_structure: List[TemplateStructure]) -> Dict[str, List[ChannelInfo]]:
        """严格按照模板结构分类频道"""
        categorized = {}
        current_category = None
        
        # 初始化分类结构
        for item in template_structure:
            if item.type == "category":
                categorized[item.name] = []
                current_category = item.name
        
        # 分配频道到分类
        for template_item in template_structure:
            if template_item.type == "channel":
                channel_name = template_item.name
                category_name = template_item.category
                
                # 查找对应的源频道数据
                matched_channel = next((ch for ch in valid_channels if ch.name == channel_name), None)
                
                if matched_channel and category_name in categorized:
                    categorized[category_name].append(matched_channel)
        
        # 移除空分类
        empty_categories = [cat for cat, channels in categorized.items() if not channels]
        for empty_cat in empty_categories:
            del categorized[empty_cat]
            Console.print_warning(f"移除空分类: {empty_cat}")
        
        return categorized
    
    @staticmethod
    def limit_interfaces_per_channel(categorized_channels: Dict[str, List[ChannelInfo]]) -> Dict[str, List[ChannelInfo]]:
        """限制单频道接口数量"""
        limited_channels = {}
        
        for category, channels in categorized_channels.items():
            # 按频道名分组
            channel_groups = {}
            for channel_data in channels:
                name = channel_data.name
                if name not in channel_groups:
                    channel_groups[name] = []
                channel_groups[name].append(channel_data)
            
            # 每个频道保留最佳接口
            limited_list = []
            for name, interfaces in channel_groups.items():
                interfaces.sort(key=lambda x: x.delay)
                limited_list.extend(interfaces[:Config.MAX_INTERFACES_PER_CHANNEL])
            
            limited_channels[category] = limited_list
        
        return limited_channels

class OutputGenerator:
    """输出生成器"""
    
    @staticmethod
    def generate_txt_output(categorized_channels: Dict[str, List[ChannelInfo]],
                          template_structure: List[TemplateStructure]) -> bool:
        """生成TXT格式输出"""
        lines = [
            f"# IPTV频道列表（生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}）",
            f"# 总频道数：{sum(len(channels) for channels in categorized_channels.values())}",
            f"# 分类数：{len(categorized_channels)}",
            f"# 严格按照模板排序，只保留模板内频道，不包含其他频道",
        ]
        
        if Config.RESOLUTION_FILTER["enable"]:
            lines.append(f"# 分辨率过滤：最小 {Config.RESOLUTION_FILTER['min_width']}x{Config.RESOLUTION_FILTER['min_height']}")
        
        lines.append("")
        
        current_category = None
        for item in template_structure:
            if item.type == "category":
                current_category = item.name
                if current_category in categorized_channels and categorized_channels[current_category]:
                    lines.append(f"{current_category},{Config.CATEGORY_MARKER}")
            
            elif item.type == "channel":
                channel_name = item.name
                if current_category and current_category in categorized_channels:
                    channel_data = next((ch for ch in categorized_channels[current_category] if ch.name == channel_name), None)
                    if channel_data:
                        if channel_data.resolution != "unknown" and channel_data.quality != ResolutionQuality.LOW_QUALITY:
                            lines.append(f"{channel_data.name} [{channel_data.resolution}],{channel_data.url}")
                        else:
                            lines.append(f"{channel_data.name},{channel_data.url}")
        
        lines.append("")
        
        success = FileUtils.write_file(Config.TXT_OUTPUT, "\n".join(lines))
        if success:
            Console.print_success(f"TXT文件生成成功：{os.path.abspath(Config.TXT_OUTPUT)}")
        return success
    
    @staticmethod
    def generate_m3u_output(categorized_channels: Dict[str, List[ChannelInfo]],
                          template_structure: List[TemplateStructure]) -> bool:
        """生成M3U格式输出"""
        lines = [
            "#EXTM3U",
            f"# Generated by IPTV Tool at {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"# Strict Template Ordering - No Other Channels",
        ]
        
        if Config.RESOLUTION_FILTER["enable"]:
            lines.append(f"# Resolution Filter: min {Config.RESOLUTION_FILTER['min_width']}x{Config.RESOLUTION_FILTER['min_height']}")
        
        current_category = None
        for item in template_structure:
            if item.type == "category":
                current_category = item.name
            
            elif item.type == "channel":
                channel_name = item.name
                if current_category and current_category in categorized_channels:
                    channel_data = next((ch for ch in categorized_channels[current_category] if ch.name == channel_name), None)
                    if channel_data:
                        if channel_data.resolution != "unknown" and channel_data.quality != ResolutionQuality.LOW_QUALITY:
                            display_name = f"{channel_data.name} [{channel_data.resolution}]"
                        else:
                            display_name = channel_data.name
                        
                        lines.extend([
                            f'#EXTINF:-1 group-title="{current_category}",{display_name}',
                            channel_data.url
                        ])
        
        success = FileUtils.write_file(Config.M3U_OUTPUT, "\n".join(lines))
        if success:
            Console.print_success(f"M3U文件生成成功：{os.path.abspath(Config.M3U_OUTPUT)}")
        return success
    
    @staticmethod
    def print_statistics(categorized_channels: Dict[str, List[ChannelInfo]],
                       template_structure: List[TemplateStructure]):
        """打印统计信息"""
        Console.print_separator("📊 生成统计")
        
        total_channels = sum(len(channels) for channels in categorized_channels.values())
        template_channel_count = len([x for x in template_structure if x.type == "channel"])
        
        Console.print_info("模板匹配情况：")
        Console.print(f"  ├─ 模板频道数：{template_channel_count}", 'green')
        Console.print(f"  ├─ 实际匹配数：{total_channels}", 'green')
        Console.print(f"  └─ 匹配成功率：{total_channels/template_channel_count*100:.1f}%", 'yellow')
        
        Console.print_info("频道分布：")
        for category, channels in categorized_channels.items():
            if channels:
                Console.print(f"  ├─ {category:<15}：{len(channels):>3} 个频道", 'green')
        
        Console.print_info("汇总信息：")
        Console.print(f"  ├─ 总频道数：{total_channels}", 'green')
        Console.print(f"  ├─ 分类数量：{len([c for c in categorized_channels.values() if c])}", 'green')
        Console.print(f"  └─ 输出文件：{Config.TXT_OUTPUT}, {Config.M3U_OUTPUT}", 'green')
        Console.print_info("提示：输出文件只包含模板内明确列出的频道，不包含任何其他频道")

# ======================== 主程序 =========================
class IPTVProcessor:
    """IPTV处理器主类"""
    
    def __init__(self):
        self.source_fetcher = SourceFetcher()
        self.channel_processor = ChannelProcessor()
        self.resolution_filter = ResolutionFilter()
        self.template_matcher = TemplateMatcher()
        self.output_generator = OutputGenerator()
    
    def process(self) -> bool:
        """主处理流程"""
        Console.print_separator("🎬 IPTV源处理工具启动 - 优化版")
        
        # 1. 配置验证
        if not Config.validate():
            return False
        
        # 2. 网络检查
        if not NetworkUtils.check_connectivity():
            return False
        
        # 3. 读取模板
        Console.print_separator("📋 读取模板")
        template_categories, all_template_channels, template_structure = TemplateManager.read_template_strict()
        if not template_structure:
            return False
        
        # 4. 抓取源数据
        Console.print_separator("🌐 抓取源数据")
        sources_content = self.source_fetcher.fetch_all_sources()
        if not sources_content:
            Console.print_error("未获取到有效源数据")
            return False
        
        # 5. 解析频道
        Console.print_separator("📋 解析频道")
        all_channels = []
        for content in sources_content:
            all_channels.extend(self.channel_processor.parse_channels(content))
        
        Console.print_success(f"解析完成 | 原始频道数：{len(all_channels)}")
        if not all_channels:
            Console.print_error("未解析到有效频道")
            return False
        
        # 6. 测速筛选
        Console.print_separator("⚡ 频道测速")
        valid_channels = self.channel_processor.speed_test_channels(all_channels)
        if not valid_channels:
            Console.print_error("无有效频道通过测速")
            return False
        
        # 7. 严格模板匹配
        Console.print_separator("🔍 严格模板匹配")
        template_filtered_channels = self.template_matcher.filter_channels_by_template(
            valid_channels, all_template_channels, template_structure
        )
        if not template_filtered_channels:
            Console.print_error("无频道匹配模板要求")
            return False
        
        # 8. 分辨率过滤
        if Config.RESOLUTION_FILTER["enable"]:
            Console.print_separator("🖥️ 严格分辨率过滤")
            resolution_filtered_channels = self.resolution_filter.filter_by_resolution(template_filtered_channels)
            if not resolution_filtered_channels:
                Console.print_error("无频道通过分辨率过滤")
                return False
        else:
            resolution_filtered_channels = template_filtered_channels
        
        # 9. 严格分类
        Console.print_separator("📂 严格模板分类")
        categorized_channels = self.template_matcher.categorize_channels_strict(
            resolution_filtered_channels, template_structure
        )
        limited_channels = self.template_matcher.limit_interfaces_per_channel(categorized_channels)
        
        if not any(limited_channels.values()):
            Console.print_error("无有效频道通过所有过滤条件")
            return False
        
        # 10. 生成输出
        Console.print_separator("💾 生成输出")
        txt_success = self.output_generator.generate_txt_output(limited_channels, template_structure)
        m3u_success = self.output_generator.generate_m3u_output(limited_channels, template_structure)
        
        if not (txt_success or m3u_success):
            Console.print_error("输出文件生成失败")
            return False
        
        # 11. 显示统计
        self.output_generator.print_statistics(limited_channels, template_structure)
        Console.print_success("IPTV严格模板处理完成！")
        Console.print_info("提示：输出文件严格按照 demo.txt 模板顺序排列，只包含模板内的频道，不包含其他任何频道")
        
        return True

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
        logging.critical(f"主程序异常：{str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
