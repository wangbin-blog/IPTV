#!/usr/bin/env python3
"""
IPTV智能管理工具 - 完整企业级版本
功能：多源抓取、频道匹配、速度测试、播放列表生成、配置管理、数据验证
版本：v8.0 (完整企业级版本)
"""

import requests
import pandas as pd
import re
import os
import time
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import hashlib
import json
from datetime import datetime
import shutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('iptv_manager.log', encoding='utf-8', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('IPTVManager')

# 可选依赖处理
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    logger.warning("TQDM未安装，将使用简单进度显示")

@dataclass
class AppConfig:
    """应用配置类"""
    source_urls: List[str]
    request_timeout: int = 15
    max_sources_per_channel: int = 8
    speed_test_timeout: int = 5
    similarity_threshold: int = 50
    max_workers: int = 6
    template_file: str = "demo.txt"
    output_txt: str = "iptv.txt"
    output_m3u: str = "iptv.m3u"
    temp_dir: str = "temp"
    cache_enabled: bool = True
    cache_expiry: int = 3600  # 缓存过期时间（秒）
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppConfig':
        """从字典创建配置"""
        return cls(**data)

class IPTVManager:
    """IPTV智能管理工具主类"""
    
    def __init__(self, config: Optional[AppConfig] = None):
        """初始化IPTV管理器"""
        # 使用默认配置或传入配置
        self.config = config or AppConfig(
            source_urls=[
                "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
                "https://live.zbds.top/tv/iptv6.txt", 
                "https://live.zbds.top/tv/iptv4.txt",
                "http://home.jundie.top:81/top/tvbox.txt",
                "https://mirror.ghproxy.com/https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
            ]
        )
        
        # 初始化会话和目录
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 创建必要的目录
        self._setup_directories()
        
        # 编译正则表达式
        self._compile_patterns()
        
        # 状态变量
        self.ffmpeg_available = False
        self.processed_count = 0
        self.total_count = 0
        self.cache_dir = Path("cache")
        self.backup_dir = Path("backups")
        self.checkpoint_file = Path("checkpoint.json")
        self.current_stage = "not_started"
        
        # 创建目录
        self.cache_dir.mkdir(exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)

    def _setup_directories(self) -> None:
        """设置必要的目录"""
        try:
            # 创建临时目录
            temp_path = Path(self.config.temp_dir)
            temp_path.mkdir(exist_ok=True)
            
            # 确保输出目录存在
            output_dir = Path(".").absolute()
            logger.info(f"工作目录: {output_dir}")
            
        except Exception as e:
            logger.error(f"目录设置失败: {e}")
            raise

    def _compile_patterns(self) -> None:
        """编译正则表达式模式"""
        self.patterns = {
            'ipv4': re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}'),
            'ipv6': re.compile(r'^https?://\[([a-fA-F0-9:]+)\]'),
            'extinf': re.compile(r'#EXTINF:.*?tvg-name="([^"]+)".*?,(.+)'),
            'category': re.compile(r'^(.*?),#genre#$'),
            'url': re.compile(r'https?://[^\s,]+'),
            'channel_name': re.compile(r'[#EXTINF:].*?,(.+)$'),
            'clean_name': re.compile(r'[^\w\u4e00-\u9fa5\s-]'),
            'tvg_name': re.compile(r'tvg-name="([^"]*)"'),
            'tvg_id': re.compile(r'tvg-id="([^"]*)"'),
            'group_title': re.compile(r'group-title="([^"]*)"'),
            'extinf_content': re.compile(r',\s*(.+)$')
        }

    def save_config(self, config_path: str = "iptv_config.json") -> bool:
        """保存配置到JSON文件"""
        try:
            config_dict = self.config.to_dict()
            
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config_dict, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ 配置已保存到: {Path(config_path).absolute()}")
            return True
        except Exception as e:
            logger.error(f"❌ 保存配置失败: {e}")
            return False

    def load_config(self, config_path: str = "iptv_config.json") -> Optional[AppConfig]:
        """从JSON文件加载配置"""
        try:
            if not Path(config_path).exists():
                logger.warning(f"⚠️ 配置文件不存在: {config_path}")
                return None
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config_dict = json.load(f)
            
            # 验证必需的配置项
            required_keys = ['source_urls']
            for key in required_keys:
                if key not in config_dict:
                    logger.error(f"❌ 配置文件中缺少必需的键: {key}")
                    return None
            
            # 创建配置对象
            config = AppConfig.from_dict(config_dict)
            
            logger.info(f"✅ 配置已从文件加载: {config_path}")
            return config
        except Exception as e:
            logger.error(f"❌ 加载配置失败: {e}")
            return None

    def validate_config(self) -> bool:
        """验证配置的完整性"""
        try:
            config = self.config
            
            # 验证URLs
            if not config.source_urls:
                logger.error("❌ 配置错误: 没有源URL")
                return False
                
            for url in config.source_urls:
                if not self.validate_url(url):
                    logger.error(f"❌ 配置错误: 无效的源URL - {url}")
                    return False
            
            # 验证数值参数
            if config.request_timeout <= 0:
                logger.error("❌ 配置错误: 请求超时必须大于0")
                return False
                
            if config.speed_test_timeout <= 0:
                logger.error("❌ 配置错误: 测速超时必须大于0")
                return False
                
            if config.similarity_threshold < 0 or config.similarity_threshold > 100:
                logger.error("❌ 配置错误: 相似度阈值必须在0-100之间")
                return False
                
            if config.max_sources_per_channel <= 0:
                logger.error("❌ 配置错误: 最大源数必须大于0")
                return False
            
            if config.max_workers <= 0:
                logger.error("❌ 配置错误: 工作线程数必须大于0")
                return False
            
            # 验证文件路径
            template_path = Path(config.template_file)
            if not template_path.exists():
                logger.warning(f"⚠️ 模板文件不存在: {template_path}")
                # 这里不返回False，因为程序会创建示例模板
            
            # 验证目录权限
            try:
                temp_dir = Path(config.temp_dir)
                temp_dir.mkdir(exist_ok=True)
                test_file = temp_dir / "test_write"
                test_file.touch()
                test_file.unlink()
            except Exception as e:
                logger.error(f"❌ 配置错误: 无法写入临时目录 - {e}")
                return False
                
            logger.info("✅ 配置验证通过")
            return True
            
        except Exception as e:
            logger.error(f"❌ 配置验证失败: {e}")
            return False

    def check_dependencies(self) -> bool:
        """检查必要的依赖"""
        try:
            # 检查基础依赖
            import requests
            import pandas as pd
            logger.info("✅ 基础依赖检查通过")
            
            # 检查FFmpeg
            self.ffmpeg_available = self._check_ffmpeg()
            
            return True
            
        except ImportError as e:
            logger.error(f"❌ 缺少依赖: {e}")
            print("请运行: pip install requests pandas")
            return False

    def _check_ffmpeg(self) -> bool:
        """检查FFmpeg是否可用"""
        try:
            result = subprocess.run(
                ['ffmpeg', '-version'], 
                capture_output=True, 
                timeout=5,
                check=False
            )
            if result.returncode == 0:
                logger.info("✅ FFmpeg可用")
                return True
            else:
                logger.warning("⚠️ FFmpeg未安装或不可用，将使用HTTP测速")
                return False
        except (subprocess.SubprocessError, FileNotFoundError, subprocess.TimeoutExpired):
            logger.warning("⚠️ FFmpeg未安装，将使用HTTP测速")
            return False

    def get_cache_key(self, url: str) -> str:
        """生成缓存键"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()

    def get_cached_content(self, url: str) -> Optional[str]:
        """获取缓存内容"""
        if not self.config.cache_enabled:
            return None
            
        cache_file = self.cache_dir / f"{self.get_cache_key(url)}.cache"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                # 检查缓存是否过期
                cache_time = cache_data.get('timestamp', 0)
                if time.time() - cache_time < self.config.cache_expiry:
                    logger.debug(f"使用缓存: {url}")
                    return cache_data.get('content')
                else:
                    logger.debug(f"缓存已过期: {url}")
            except Exception as e:
                logger.debug(f"读取缓存失败: {e}")
                
        return None

    def set_cached_content(self, url: str, content: str) -> None:
        """设置缓存内容"""
        if not self.config.cache_enabled:
            return
            
        try:
            cache_file = self.cache_dir / f"{self.get_cache_key(url)}.cache"
            cache_data = {
                'timestamp': time.time(),
                'content': content,
                'url': url
            }
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False)
        except Exception as e:
            logger.debug(f"写入缓存失败: {e}")

    def backup_data(self, stage: str, data: Any) -> bool:
        """备份处理阶段的数据"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = self.backup_dir / f"backup_{stage}_{timestamp}.json"
            
            if isinstance(data, pd.DataFrame):
                # 备份DataFrame
                data.to_json(backup_file, orient='records', force_ascii=False, indent=2)
            elif isinstance(data, dict):
                # 备份字典
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            elif isinstance(data, str):
                # 备份字符串
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump({'content': data}, f, ensure_ascii=False, indent=2)
            else:
                # 备份其他数据类型
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump({'data': str(data)}, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"✅ 数据备份完成: {stage} -> {backup_file}")
            return True
        except Exception as e:
            logger.error(f"❌ 数据备份失败: {e}")
            return False

    def save_checkpoint(self, stage: str, data: Any = None) -> bool:
        """保存处理检查点"""
        try:
            checkpoint_data = {
                'stage': stage,
                'timestamp': time.time(),
                'config': self.config.to_dict()
            }
            
            if data is not None:
                # 保存关键数据摘要
                if isinstance(data, pd.DataFrame):
                    checkpoint_data['data_summary'] = {
                        'rows': len(data),
                        'columns': list(data.columns),
                        'sample_channels': data['program_name'].head(5).tolist() if 'program_name' in data.columns else []
                    }
                elif isinstance(data, dict):
                    checkpoint_data['data_summary'] = {
                        'categories': len(data),
                        'total_channels': sum(len(channels) for channels in data.values()) if data else 0
                    }
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
            
            self.current_stage = stage
            logger.info(f"✅ 检查点保存: {stage}")
            return True
        except Exception as e:
            logger.error(f"❌ 检查点保存失败: {e}")
            return False

    def can_resume_from_checkpoint(self) -> Tuple[bool, Optional[Dict]]:
        """检查是否可以从检查点恢复"""
        try:
            if not self.checkpoint_file.exists():
                return False, None
            
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            
            # 验证检查点数据的完整性
            required_keys = ['stage', 'timestamp']
            for key in required_keys:
                if key not in checkpoint_data:
                    return False, None
            
            # 检查时间戳是否在合理范围内（24小时内）
            if time.time() - checkpoint_data['timestamp'] > 86400:
                logger.warning("⚠️ 检查点已过期（超过24小时）")
                return False, None
            
            return True, checkpoint_data
        except Exception as e:
            logger.error(f"❌ 检查点读取失败: {e}")
            return False, None

    def simple_progress_bar(self, iterable, desc: str = "Processing", total: Optional[int] = None):
        """完整的简单进度条实现"""
        if iterable is None:
            logger.error(f"{desc}: iterable 为 None")
            return
            
        try:
            # 尝试获取总数
            if total is None:
                try:
                    total = len(iterable)
                except (TypeError, AttributeError):
                    total = 0
                    logger.warning(f"无法确定 {desc} 的总数")
            
            processed = 0
            start_time = time.time()
            
            for item in iterable:
                if item is None:
                    continue
                    
                yield item
                processed += 1
                
                # 计算进度和预计时间
                if total > 0:
                    percent = min(100, (processed / total) * 100)
                    elapsed = time.time() - start_time
                    if processed > 0:
                        eta = (elapsed / processed) * (total - processed)
                        eta_str = f"ETA: {eta:.1f}s"
                    else:
                        eta_str = "计算中..."
                    
                    bar_length = 50
                    filled_length = int(bar_length * percent / 100)
                    bar = '█' * filled_length + ' ' * (bar_length - filled_length)
                    display_text = f"\r{desc}: [{bar}] {percent:.1f}% ({processed}/{total}) {eta_str}"
                else:
                    display_text = f"\r{desc}: 已处理 {processed} 项"
                
                print(display_text, end="", flush=True)
                
            print()  # 完成后换行
            
        except Exception as e:
            logger.error(f"进度条错误: {e}")
            # 降级处理：直接返回迭代器
            for item in iterable:
                yield item

    def validate_url(self, url: str) -> bool:
        """验证URL格式是否正确"""
        if not url or not isinstance(url, str):
            return False
            
        try:
            result = urlparse(url)
            valid_scheme = result.scheme in ['http', 'https']
            valid_netloc = bool(result.netloc)
            valid_domain = '.' in result.netloc or 'localhost' in result.netloc or '[' in result.netloc
            
            return all([valid_scheme, valid_netloc, valid_domain])
        except Exception as e:
            logger.debug(f"URL解析失败: {url} - {e}")
            return False

    def fetch_streams_from_url(self, url: str) -> Optional[str]:
        """从URL获取流数据"""
        if not self.validate_url(url):
            logger.error(f"❌ 无效的URL: {url}")
            return None
            
        # 检查缓存
        cached_content = self.get_cached_content(url)
        if cached_content:
            return cached_content
            
        logger.info(f"📡 正在爬取源: {url}")
        try:
            response = self.session.get(url, timeout=self.config.request_timeout)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                content = response.text
                content_length = len(content)
                logger.info(f"✅ 成功获取数据: {url} ({content_length} 字符)")
                
                # 缓存内容
                self.set_cached_content(url, content)
                return content
            else:
                logger.error(f"❌ 获取数据失败，状态码: {response.status_code} - {url}")
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ 请求超时: {url}")
        except requests.exceptions.ConnectionError:
            logger.error(f"❌ 连接错误: {url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 请求错误: {e} - {url}")
        except Exception as e:
            logger.error(f"❌ 未知错误: {e} - {url}")
            
        return None

    def fetch_all_streams(self) -> str:
        """获取所有源的流数据（使用进度条）"""
        logger.info("🚀 开始智能多源抓取...")
        
        if not self.config.source_urls:
            logger.error("❌ 没有配置源URL")
            return ""
        
        all_streams = []
        successful_sources = 0
        
        def process_future(future, url: str) -> bool:
            """处理单个future结果"""
            nonlocal successful_sources
            try:
                content = future.result()
                if content:
                    all_streams.append(content)
                    successful_sources += 1
                    return True
            except Exception as e:
                logger.error(f"处理 {url} 时发生错误: {e}")
            return False
        
        if TQDM_AVAILABLE:
            with tqdm(total=len(self.config.source_urls), desc="🌐 抓取源数据", unit="source") as pbar:
                with ThreadPoolExecutor(max_workers=min(5, len(self.config.source_urls))) as executor:
                    # 创建future到URL的映射
                    future_to_url = {}
                    for url in self.config.source_urls:
                        future = executor.submit(self.fetch_streams_from_url, url)
                        future_to_url[future] = url
                    
                    for future in as_completed(future_to_url):
                        url = future_to_url[future]
                        process_future(future, url)
                        pbar.set_postfix({
                            "成功": successful_sources, 
                            "当前源": url[:30] + "..." if len(url) > 30 else url
                        })
                        pbar.update(1)
        else:
            print(f"总共 {len(self.config.source_urls)} 个源需要抓取")
            with ThreadPoolExecutor(max_workers=min(5, len(self.config.source_urls))) as executor:
                # 创建future列表和映射
                futures = []
                future_url_map = {}
                
                for url in self.config.source_urls:
                    future = executor.submit(self.fetch_streams_from_url, url)
                    futures.append(future)
                    future_url_map[future] = url
                
                # 处理完成的任务
                for future in self.simple_progress_bar(as_completed(futures), "抓取进度", len(futures)):
                    url = future_url_map.get(future, "未知URL")
                    process_future(future, url)
    
        logger.info(f"✅ 成功获取 {successful_sources}/{len(self.config.source_urls)} 个源的数据")
        return "\n".join(all_streams) if all_streams else ""

    def _extract_program_name(self, extinf_line: str) -> str:
        """完整的EXTINF行解析"""
        if not extinf_line.startswith('#EXTINF'):
            return "未知频道"
        
        try:
            # 方法1: 从tvg-name属性提取（最高优先级）
            tvg_match = self.patterns['tvg_name'].search(extinf_line)
            if tvg_match and tvg_match.group(1).strip():
                name = tvg_match.group(1).strip()
                if name and name != "未知频道":
                    return name
            
            # 方法2: 从逗号后的内容提取
            content_match = self.patterns['extinf_content'].search(extinf_line)
            if content_match and content_match.group(1).strip():
                name = content_match.group(1).strip()
                # 清理可能的额外信息
                name = re.sub(r'\[.*?\]|\(.*?\)', '', name).strip()
                if name and name != "未知频道":
                    return name
            
            # 方法3: 尝试其他属性
            for attr_pattern in [self.patterns['tvg_id'], self.patterns['group_title']]:
                attr_match = attr_pattern.search(extinf_line)
                if attr_match and attr_match.group(1).strip():
                    name = attr_match.group(1).strip()
                    if name and name != "未知频道":
                        return name
                        
        except Exception as e:
            logger.debug(f"EXTINF解析错误: {extinf_line} - {e}")
        
        return "未知频道"

    def parse_m3u(self, content: str) -> List[Dict[str, str]]:
        """解析M3U格式内容"""
        if not content or not isinstance(content, str):
            return []
            
        streams = []
        lines = content.splitlines()
        current_program = None
        current_group = "默认分组"
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if not line:
                i += 1
                continue
                
            if line.startswith("#EXTINF"):
                # 提取频道信息
                current_program = self._extract_program_name(line)
                
                # 提取分组信息
                group_match = self.patterns['group_title'].search(line)
                if group_match:
                    current_group = group_match.group(1).strip()
                else:
                    current_group = "默认分组"
                    
                # 查找下一行的URL
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if self.validate_url(next_line):
                        streams.append({
                            "program_name": current_program,
                            "stream_url": next_line,
                            "group": current_group,
                            "original_name": current_program
                        })
                        i += 1  # 跳过URL行
            elif line.startswith(('http://', 'https://')):
                # 独立的URL行（没有EXTINF信息）
                if self.validate_url(line):
                    streams.append({
                        "program_name": "未知频道",
                        "stream_url": line,
                        "group": "默认分组",
                        "original_name": "未知频道"
                    })
            
            i += 1
        
        return streams

    def parse_txt(self, content: str) -> List[Dict[str, str]]:
        """解析TXT格式内容"""
        if not content or not isinstance(content, str):
            return []
            
        streams = []
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '#genre#' in line:
                continue
            
            # 支持多种分隔符格式
            if ',' in line:
                parts = line.split(',', 1)
                if len(parts) == 2:
                    program_name = parts[0].strip()
                    url_part = parts[1].strip()
                    
                    # 从第二部分提取URL
                    url_match = self.patterns['url'].search(url_part)
                    if url_match:
                        stream_url = url_match.group()
                        if self.validate_url(stream_url):
                            streams.append({
                                "program_name": program_name,
                                "stream_url": stream_url,
                                "group": "默认分组",
                                "original_name": program_name
                            })
            else:
                # 尝试从整行提取URL
                url_match = self.patterns['url'].search(line)
                if url_match:
                    stream_url = url_match.group()
                    program_name = line.replace(stream_url, '').strip()
                    if not program_name:
                        program_name = "未知频道"
                    
                    if self.validate_url(stream_url):
                        streams.append({
                            "program_name": program_name,
                            "stream_url": stream_url,
                            "group": "默认分组",
                            "original_name": program_name
                        })
        
        return streams

    def organize_streams(self, content: str) -> pd.DataFrame:
        """整理流数据，去除重复和无效数据"""
        if not content:
            logger.error("❌ 没有内容可处理")
            return pd.DataFrame()
            
        logger.info("🔍 解析流数据...")
        
        try:
            # 自动检测格式并解析
            if content.startswith("#EXTM3U"):
                streams = self.parse_m3u(content)
            else:
                streams = self.parse_txt(content)
            
            if not streams:
                logger.error("❌ 未能解析出任何流数据")
                return pd.DataFrame()
                
            # 转换为DataFrame
            df = pd.DataFrame(streams)
            
            # 数据清理
            initial_count = len(df)
            
            # 移除空值
            df = df.dropna()
            
            # 过滤无效的节目名称和URL
            df = df[df['program_name'].str.len() > 0]
            df = df[df['stream_url'].str.startswith(('http://', 'https://'))]
            
            # 应用URL验证
            df['url_valid'] = df['stream_url'].apply(self.validate_url)
            df = df[df['url_valid']].drop('url_valid', axis=1)
            
            # 确保original_name列存在
            if 'original_name' not in df.columns:
                df['original_name'] = df['program_name']
            
            # 去重（基于节目名称和URL）
            df = df.drop_duplicates(subset=['program_name', 'stream_url'])
            
            final_count = len(df)
            logger.info(f"📊 数据清理: {initial_count} -> {final_count} 个流")
            
            if final_count == 0:
                logger.warning("⚠️ 数据清理后没有有效的流数据")
                
            return df
            
        except Exception as e:
            logger.error(f"❌ 数据处理错误: {e}")
            return pd.DataFrame()

    def load_template(self) -> Optional[Dict[str, List[str]]]:
        """加载频道模板文件"""
        template_file = Path(self.config.template_file)
        
        if not template_file.exists():
            logger.error(f"❌ 模板文件 {template_file} 不存在")
            return None
            
        logger.info(f"📋 加载模板文件: {template_file}")
        categories = {}
        current_category = None
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    # 检测分类行
                    category_match = self.patterns['category'].match(line)
                    if category_match:
                        current_category = category_match.group(1).strip()
                        categories[current_category] = []
                        logger.debug(f"找到分类: {current_category}")
                    
                    # 检测频道行
                    elif current_category and line and not line.startswith('#'):
                        # 提取频道名称（去除可能的分隔符和注释）
                        channel_name = line.split(',')[0].strip() if ',' in line else line.strip()
                        if channel_name:
                            categories[current_category].append(channel_name)
                            logger.debug(f"添加频道: {channel_name} -> {current_category}")
        
        except Exception as e:
            logger.error(f"❌ 读取模板文件失败: {e}")
            return None
        
        if not categories:
            logger.error("❌ 模板文件中未找到有效的频道分类")
            return None
            
        # 统计信息
        total_channels = sum(len(channels) for channels in categories.values())
        logger.info(f"📁 模板分类: {list(categories.keys())}")
        logger.info(f"📺 模板频道总数: {total_channels}")
        
        return categories

    def clean_channel_name(self, name: str) -> str:
        """改进的频道名称清理"""
        if not name:
            return ""
        
        try:
            # 保留关键信息：中文、英文、数字、空格、横杠
            cleaned = re.sub(r'[^\w\u4e00-\u9fa5\s-]', '', name.lower())
            # 合并多个空格
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            # 移除常见的无意义后缀
            cleaned = re.sub(r'\s+(hd|fhd|4k|直播|频道|tv|television)$', '', cleaned)
            return cleaned
        except Exception as e:
            logger.debug(f"频道名称清理错误: {name} - {e}")
            return name.lower() if name else ""

    def similarity_score(self, str1: str, str2: str) -> int:
        """计算两个字符串的相似度分数（0-100）"""
        if not str1 or not str2 or not isinstance(str1, str) or not isinstance(str2, str):
            return 0
            
        try:
            # 清理字符串
            clean_str1 = self.clean_channel_name(str1)
            clean_str2 = self.clean_channel_name(str2)
            
            if not clean_str1 or not clean_str2:
                return 0
            
            # 完全匹配
            if clean_str1 == clean_str2:
                return 100
            
            # 包含关系（双向）
            if clean_str1 in clean_str2:
                return 90
            if clean_str2 in clean_str1:
                return 85
            
            # 使用集合计算Jaccard相似度
            set1 = set(clean_str1)
            set2 = set(clean_str2)
            
            intersection = len(set1 & set2)
            union = len(set1 | set2)
            
            if union > 0:
                jaccard_similarity = intersection / union
                return int(jaccard_similarity * 80)
                
        except Exception as e:
            logger.debug(f"相似度计算错误: {str1}, {str2} - {e}")
        
        return 0

    def filter_and_sort_sources(self, sources_df: pd.DataFrame, template_channels: List[str]) -> pd.DataFrame:
        """完整的频道匹配和源筛选实现"""
        logger.info("🎯 开始频道匹配和源筛选...")
        
        # 严格的空值检查
        if sources_df is None or sources_df.empty:
            logger.error("❌ 源数据为空或None，无法进行匹配")
            return pd.DataFrame()
        
        if template_channels is None or not template_channels:
            logger.error("❌ 模板频道列表为空或None")
            return pd.DataFrame()
        
        # 检查必要的列是否存在
        required_columns = ['program_name', 'stream_url']
        missing_columns = [col for col in required_columns if col not in sources_df.columns]
        if missing_columns:
            logger.error(f"❌ 源数据缺少必要列: {missing_columns}")
            return pd.DataFrame()
        
        # 创建匹配结果列表
        matched_results = []
        
        logger.info(f"开始匹配 {len(template_channels)} 个模板频道...")
        
        if TQDM_AVAILABLE:
            with tqdm(total=len(template_channels), desc="🔍 频道匹配", unit="channel") as pbar:
                for template_channel in template_channels:
                    best_match_row = None
                    best_score = 0
                    
                    # 为每个模板频道找到最佳匹配的源
                    for _, source_row in sources_df.iterrows():
                        source_channel = source_row['program_name']
                        score = self.similarity_score(template_channel, source_channel)
                        
                        if score > best_score and score >= self.config.similarity_threshold:
                            best_score = score
                            best_match_row = source_row.copy()
                            best_match_row['template_channel'] = template_channel
                            best_match_row['match_score'] = score
                    
                    if best_match_row is not None:
                        matched_results.append(best_match_row)
                        pbar.set_postfix({
                            "匹配度": f"{best_score}%", 
                            "频道": template_channel[:20]
                        })
                    
                    pbar.update(1)
        else:
            for template_channel in self.simple_progress_bar(template_channels, "频道匹配"):
                best_match_row = None
                best_score = 0
                
                for _, source_row in sources_df.iterrows():
                    source_channel = source_row['program_name']
                    score = self.similarity_score(template_channel, source_channel)
                    
                    if score > best_score and score >= self.config.similarity_threshold:
                        best_score = score
                        best_match_row = source_row.copy()
                        best_match_row['template_channel'] = template_channel
                        best_match_row['match_score'] = score
                
                if best_match_row is not None:
                    matched_results.append(best_match_row)
        
        # 转换为DataFrame并整合数据
        if matched_results:
            result_df = pd.DataFrame(matched_results)
            
            # 确保必要列存在
            required_columns = ['program_name', 'stream_url', 'template_channel', 'match_score']
            for col in required_columns:
                if col not in result_df.columns:
                    logger.warning(f"缺失列: {col}，使用默认值填充")
                    if col == 'template_channel':
                        result_df[col] = result_df.get('program_name', '未知频道')
                    elif col == 'match_score':
                        result_df[col] = 0
            
            # 重命名列以明确含义
            column_mapping = {
                'program_name': 'original_name',
                'template_channel': 'program_name'
            }
            result_df = result_df.rename(columns=column_mapping)
            
            # 确保original_name列存在
            if 'original_name' not in result_df.columns:
                result_df['original_name'] = result_df.get('program_name', '未知频道')
            
            # 重新排列列顺序
            preferred_order = ['program_name', 'original_name', 'stream_url', 'match_score', 'group']
            available_columns = [col for col in preferred_order if col in result_df.columns]
            other_columns = [col for col in result_df.columns if col not in preferred_order]
            result_df = result_df[available_columns + other_columns]
            
            # 显示匹配结果统计
            unique_matched_channels = result_df['program_name'].nunique()
            logger.info(f"✅ 频道匹配完成: {len(matched_results)} 个流匹配到 {unique_matched_channels} 个模板频道")
            
            # 显示最佳匹配结果
            if not result_df.empty:
                top_matches = result_df.nlargest(10, 'match_score')[['program_name', 'original_name', 'match_score']]
                print("\n📊 最佳匹配结果（前10个）:")
                for _, match in top_matches.iterrows():
                    print(f"  ✅ {match['program_name']:<20} <- {match['original_name']:<30} (分数: {match['match_score']:2d})")
            
            return result_df
        else:
            logger.error("❌ 没有找到任何匹配的频道")
            return pd.DataFrame()

    def speed_test_ffmpeg(self, stream_url: str) -> Tuple[bool, float]:
        """使用FFmpeg进行流媒体测速（更准确但较慢）"""
        if not self.ffmpeg_available or not stream_url:
            return False, float('inf')
            
        temp_file = Path(self.config.temp_dir) / f'test_{abs(hash(stream_url))}.ts'
        
        try:
            cmd = [
                'ffmpeg',
                '-y',  # 覆盖输出文件
                '-timeout', '3000000',  # 3秒超时（微秒）
                '-i', stream_url,
                '-t', '2',  # 只测试2秒
                '-c', 'copy',
                '-f', 'mpegts',
                '-max_muxing_queue_size', '1024',
                str(temp_file)
            ]
            
            start_time = time.time()
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=5,
                check=False
            )
            end_time = time.time()
            
            # 清理临时文件
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            
            if result.returncode == 0:
                speed = end_time - start_time
                return True, speed
            else:
                return False, float('inf')
                
        except (subprocess.TimeoutExpired, Exception) as e:
            # 清理临时文件
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            logger.debug(f"FFmpeg测速失败: {stream_url} - {e}")
            return False, float('inf')

    def speed_test_simple(self, stream_url: str) -> Tuple[bool, float]:
        """简单的HTTP测速（快速但不够准确）"""
        if not stream_url:
            return False, float('inf')
            
        try:
            start_time = time.time()
            response = self.session.head(
                stream_url, 
                timeout=self.config.speed_test_timeout,
                allow_redirects=True
            )
            end_time = time.time()
            
            if response.status_code in [200, 302, 301, 307]:
                return True, end_time - start_time
            else:
                return False, float('inf')
        except Exception as e:
            logger.debug(f"HTTP测速失败: {stream_url} - {e}")
            return False, float('inf')

    def speed_test_sources(self, sources_df: pd.DataFrame) -> pd.DataFrame:
        """完整的测速实现"""
        logger.info("⏱️  开始智能测速...")
        
        if sources_df is None or sources_df.empty:
            logger.error("❌ 没有需要测速的源")
            return pd.DataFrame()
            
        results = []
        total_sources = len(sources_df)
        
        def test_single_source(row) -> Dict[str, Any]:
            """测试单个源的辅助函数"""
            try:
                program_name = row['program_name']
                stream_url = row['stream_url']
                
                # 根据URL类型选择测速方法
                if any(ext in stream_url.lower() for ext in ['.m3u8', '.ts', '.flv', '.mp4']):
                    if self.ffmpeg_available:
                        accessible, speed = self.speed_test_ffmpeg(stream_url)
                    else:
                        accessible, speed = self.speed_test_simple(stream_url)
                else:
                    accessible, speed = self.speed_test_simple(stream_url)
                
                return {
                    'program_name': program_name,
                    'stream_url': stream_url,
                    'accessible': accessible,
                    'speed': speed,
                    'original_name': row.get('original_name', ''),
                    'match_score': row.get('match_score', 0)
                }
            except Exception as e:
                logger.error(f"测速单个源时出错: {e}")
                return {
                    'program_name': row.get('program_name', '未知'),
                    'stream_url': row.get('stream_url', ''),
                    'accessible': False,
                    'speed': float('inf'),
                    'original_name': row.get('original_name', ''),
                    'match_score': row.get('match_score', 0)
                }
        
        if TQDM_AVAILABLE:
            with tqdm(total=total_sources, desc="⚡ 测速进度", unit="source") as pbar:
                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    futures = [executor.submit(test_single_source, row) for _, row in sources_df.iterrows()]
                    
                    for future in as_completed(futures):
                        try:
                            result = future.result(timeout=15)
                            results.append(result)
                            status = "✅" if result['accessible'] else "❌"
                            pbar.set_postfix({
                                "状态": status, 
                                "速度": f"{result['speed']:.2f}s" if result['accessible'] else "超时",
                                "频道": result['program_name'][:15] + "..." if len(result['program_name']) > 15 else result['program_name']
                            })
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"测速异常: {e}")
                            pbar.update(1)
        else:
            print(f"总共 {total_sources} 个源需要测速")
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = [executor.submit(test_single_source, row) for _, row in sources_df.iterrows()]
                
                for future in self.simple_progress_bar(as_completed(futures), "测速进度", total_sources):
                    try:
                        result = future.result(timeout=15)
                        results.append(result)
                    except Exception as e:
                        logger.error(f"测速异常: {e}")
        
        # 转换为DataFrame并整合结果
        try:
            result_df = pd.DataFrame(results)
            if result_df.empty:
                return pd.DataFrame()
            
            # 过滤可访问的源并按速度排序
            accessible_df = result_df[result_df['accessible']].copy()
            accessible_df = accessible_df.sort_values(['program_name', 'speed'])
            
            accessible_count = len(accessible_df)
            logger.info(f"📊 测速完成: {accessible_count}/{total_sources} 个源可用")
            
            if accessible_count == 0:
                logger.warning("⚠️ 没有可用的源通过测速")
                
            return accessible_df
            
        except Exception as e:
            logger.error(f"❌ 处理测速结果时出错: {e}")
            return pd.DataFrame()

    def generate_final_data(self, speed_tested_df: pd.DataFrame, template_categories: Dict[str, List[str]]) -> Dict[str, Any]:
        """生成最终数据（使用进度条）"""
        logger.info("🎨 生成最终文件...")
        
        final_data = {}
        total_sources = 0
        
        if speed_tested_df is None or speed_tested_df.empty:
            logger.error("❌ 测速数据为空")
            return final_data
        
        if not template_categories:
            logger.error("❌ 模板分类为空")
            return final_data
        
        # 计算总频道数
        total_channels = sum(len(channels) for channels in template_categories.values())
        
        if total_channels == 0:
            logger.error("❌ 模板中没有频道")
            return final_data
        
        logger.info(f"为 {len(template_categories)} 个分类生成最终数据...")
        
        if TQDM_AVAILABLE:
            with tqdm(total=total_channels, desc="📦 生成数据", unit="channel") as pbar:
                for category, channels in template_categories.items():
                    final_data[category] = {}
                    
                    for channel in channels:
                        # 获取该频道的所有源
                        channel_sources = speed_tested_df[speed_tested_df['program_name'] == channel]
                        
                        if not channel_sources.empty:
                            # 按速度排序并取前N个
                            sorted_sources = channel_sources.head(self.config.max_sources_per_channel)
                            final_data[category][channel] = sorted_sources[['stream_url', 'speed']].to_dict('records')
                            source_count = len(sorted_sources)
                            total_sources += source_count
                            pbar.set_postfix({
                                "分类": category[:10],
                                "频道": channel[:15] + "..." if len(channel) > 15 else channel,
                                "源数": source_count
                            })
                        else:
                            final_data[category][channel] = []
                            pbar.set_postfix({
                                "分类": category[:10],
                                "频道": channel[:15] + "..." if len(channel) > 15 else channel,
                                "源数": 0
                            })
                        
                        pbar.update(1)
        else:
            current_channel = 0
            for category, channels in template_categories.items():
                final_data[category] = {}
                
                for channel in channels:
                    channel_sources = speed_tested_df[speed_tested_df['program_name'] == channel]
                    
                    if not channel_sources.empty:
                        sorted_sources = channel_sources.head(self.config.max_sources_per_channel)
                        final_data[category][channel] = sorted_sources[['stream_url', 'speed']].to_dict('records')
                        source_count = len(sorted_sources)
                        total_sources += source_count
                    else:
                        final_data[category][channel] = []
                    
                    current_channel += 1
                    percent = min(100, (current_channel / total_channels) * 100)
                    bar_length = 50
                    filled_length = int(bar_length * percent / 100)
                    bar = '█' * filled_length + ' ' * (bar_length - filled_length)
                    print(f"\r生成数据: [{bar}] {percent:.1f}% ({current_channel}/{total_channels})", end="", flush=True)
            print()
        
        logger.info(f"📦 总共收集到 {total_sources} 个有效源")
        return final_data

    def save_output_files(self, final_data: Dict[str, Any]) -> bool:
        """保存输出文件（使用进度条）"""
        logger.info("💾 保存文件...")
        
        if not final_data:
            logger.error("❌ 没有数据需要保存")
            return False
        
        # 计算总行数用于进度条
        total_lines = 0
        for category, channels in final_data.items():
            total_lines += 1  # 分类行
            for channel, sources in channels.items():
                total_lines += len(sources)  # 频道行
            total_lines += 1  # 空行
        
        success_count = 0
        
        # 保存TXT格式
        try:
            if TQDM_AVAILABLE:
                with tqdm(total=total_lines, desc="📄 保存TXT", unit="line") as pbar:
                    with open(self.config.output_txt, 'w', encoding='utf-8') as f:
                        for category, channels in final_data.items():
                            f.write(f"{category},#genre#\n")
                            pbar.update(1)
                            
                            for channel, sources in channels.items():
                                for source in sources:
                                    f.write(f"{channel},{source['stream_url']}\n")
                                    pbar.update(1)
                            
                            f.write("\n")
                            pbar.update(1)
            else:
                print("保存TXT文件...")
                with open(self.config.output_txt, 'w', encoding='utf-8') as f:
                    for category, channels in final_data.items():
                        f.write(f"{category},#genre#\n")
                        for channel, sources in channels.items():
                            for source in sources:
                                f.write(f"{channel},{source['stream_url']}\n")
                        f.write("\n")
            
            success_count += 1
            logger.info(f"✅ TXT文件已保存: {Path(self.config.output_txt).absolute()}")
            
        except Exception as e:
            logger.error(f"❌ 保存TXT文件失败: {e}")
            return False
        
        # 保存M3U格式
        try:
            if TQDM_AVAILABLE:
                with tqdm(total=total_lines, desc="📄 保存M3U", unit="line") as pbar:
                    with open(self.config.output_m3u, 'w', encoding='utf-8') as f:
                        f.write("#EXTM3U\n")
                        pbar.update(1)
                        
                        for category, channels in final_data.items():
                            for channel, sources in channels.items():
                                for source in sources:
                                    f.write(f'#EXTINF:-1 tvg-name="{channel}" group-title="{category}",{channel}\n')
                                    f.write(f"{source['stream_url']}\n")
                                    pbar.update(2)
            else:
                print("保存M3U文件...")
                with open(self.config.output_m3u, 'w', encoding='utf-8') as f:
                    f.write("#EXTM3U\n")
                    for category, channels in final_data.items():
                        for channel, sources in channels.items():
                            for source in sources:
                                f.write(f'#EXTINF:-1 tvg-name="{channel}" group-title="{category}",{channel}\n')
                                f.write(f"{source['stream_url']}\n")
            
            success_count += 1
            logger.info(f"✅ M3U文件已保存: {Path(self.config.output_m3u).absolute()}")
            
        except Exception as e:
            logger.error(f"❌ 保存M3U文件失败: {e}")
            return False
            
        return success_count == 2  # 两个文件都保存成功

    def validate_output_files(self) -> Dict[str, Any]:
        """验证输出文件的完整性和格式"""
        validation_result = {
            'txt_file': {'exists': False, 'categories': 0, 'sources': 0, 'valid': False},
            'm3u_file': {'exists': False, 'channels': 0, 'sources': 0, 'valid': False},
            'overall_valid': False
        }
        
        try:
            # 验证TXT文件
            txt_path = Path(self.config.output_txt)
            if txt_path.exists():
                validation_result['txt_file']['exists'] = True
                with open(txt_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 统计频道和源数量
                    lines = content.strip().split('\n')
                    categories = [line for line in lines if line.endswith(',#genre#')]
                    sources = [line for line in lines if line and not line.endswith(',#genre#') and ',' in line]
                    
                    validation_result['txt_file']['categories'] = len(categories)
                    validation_result['txt_file']['sources'] = len(sources)
                    validation_result['txt_file']['valid'] = len(sources) > 0 and len(categories) > 0
            
            # 验证M3U文件
            m3u_path = Path(self.config.output_m3u)
            if m3u_path.exists():
                validation_result['m3u_file']['exists'] = True
                with open(m3u_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 统计EXTINF行和URL行
                    lines = content.strip().split('\n')
                    extinf_lines = [line for line in lines if line.startswith('#EXTINF')]
                    url_lines = [line for line in lines if line.startswith(('http://', 'https://'))]
                    
                    validation_result['m3u_file']['channels'] = len(extinf_lines)
                    validation_result['m3u_file']['sources'] = len(url_lines)
                    validation_result['m3u_file']['valid'] = (len(extinf_lines) == len(url_lines) and 
                                                            len(url_lines) > 0 and
                                                            content.startswith('#EXTM3U'))
            
            # 总体验证
            validation_result['overall_valid'] = (
                validation_result['txt_file']['valid'] and 
                validation_result['m3u_file']['valid']
            )
            
            logger.info("✅ 输出文件验证完成")
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 输出文件验证失败: {e}")
            validation_result['error'] = str(e)
            return validation_result

    def generate_integrity_report(self, final_data: Dict) -> Dict[str, Any]:
        """生成数据完整性报告"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'unknown',
            'categories_analysis': {},
            'channel_coverage': {},
            'data_quality': {},
            'recommendations': []
        }
        
        try:
            if not final_data:
                report['overall_status'] = 'empty'
                report['recommendations'].append('最终数据为空，请检查源数据和模板匹配')
                return report
            
            total_categories = len(final_data)
            total_channels = 0
            total_sources = 0
            channels_with_sources = 0
            
            # 分析每个分类
            for category, channels in final_data.items():
                category_channels = 0
                category_sources = 0
                category_channels_with_sources = 0
                
                for channel, sources in channels.items():
                    total_channels += 1
                    category_channels += 1
                    
                    if sources:
                        total_sources += len(sources)
                        category_sources += len(sources)
                        channels_with_sources += 1
                        category_channels_with_sources += 1
                
                # 分类分析
                report['categories_analysis'][category] = {
                    'channels_total': category_channels,
                    'channels_with_sources': category_channels_with_sources,
                    'sources_total': category_sources,
                    'coverage_rate': round(category_channels_with_sources / category_channels * 100, 2) if category_channels > 0 else 0
                }
            
            # 总体覆盖率
            coverage_rate = round(channels_with_sources / total_channels * 100, 2) if total_channels > 0 else 0
            avg_sources_per_channel = round(total_sources / channels_with_sources, 2) if channels_with_sources > 0 else 0
            
            report['channel_coverage'] = {
                'total_categories': total_categories,
                'total_channels': total_channels,
                'channels_with_sources': channels_with_sources,
                'coverage_rate': coverage_rate,
                'total_sources': total_sources,
                'avg_sources_per_channel': avg_sources_per_channel
            }
            
            # 数据质量评估
            if coverage_rate >= 80:
                report['overall_status'] = 'excellent'
            elif coverage_rate >= 60:
                report['overall_status'] = 'good'
            elif coverage_rate >= 40:
                report['overall_status'] = 'fair'
            else:
                report['overall_status'] = 'poor'
            
            # 数据质量指标
            report['data_quality'] = {
                'coverage_score': coverage_rate,
                'source_diversity_score': min(100, avg_sources_per_channel * 20),  # 每个频道5个源得100分
                'category_balance_score': min(100, (total_categories / 10) * 100)  # 10个分类得100分
            }
            
            # 生成建议
            if coverage_rate < 50:
                report['recommendations'].append('频道覆盖率较低，建议增加源URL或调整相似度阈值')
            if avg_sources_per_channel < 2:
                report['recommendations'].append('平均源数量较少，建议增加源URL或调整最大源数限制')
            if total_categories < 3:
                report['recommendations'].append('分类数量较少，建议完善模板文件')
            
            if not report['recommendations']:
                report['recommendations'].append('数据质量良好，无需特殊调整')
            
            logger.info("✅ 完整性报告生成完成")
            return report
            
        except Exception as e:
            logger.error(f"❌ 完整性报告生成失败: {e}")
            report['overall_status'] = 'error'
            report['error'] = str(e)
            return report

    def print_statistics(self, final_data: Dict[str, Any]):
        """打印统计信息"""
        print("\n" + "="*50)
        print("📈 生成统计报告")
        print("="*50)
        
        if not final_data:
            print("❌ 没有数据可统计")
            return
        
        total_channels = 0
        total_sources = 0
        categories_with_sources = 0
        
        for category, channels in final_data.items():
            category_channels = 0
            category_sources = 0
            
            for channel, sources in channels.items():
                if sources:
                    category_channels += 1
                    category_sources += len(sources)
            
            if category_channels > 0:
                categories_with_sources += 1
                print(f"  📺 {category}: {category_channels}频道, {category_sources}源")
                total_channels += category_channels
                total_sources += category_sources
        
        print("-"*50)
        print(f"📊 总计: {total_channels}频道, {total_sources}源")
        print(f"📁 有效分类: {categories_with_sources}/{len(final_data)}")
        
        # 统计无源的频道
        no_source_channels = []
        for category, channels in final_data.items():
            for channel, sources in channels.items():
                if not sources:
                    no_source_channels.append(f"{category}-{channel}")
        
        if no_source_channels:
            print(f"⚠️  无源频道: {len(no_source_channels)}个")
            if len(no_source_channels) <= 10:
                for channel in no_source_channels[:10]:
                    print(f"    ❌ {channel}")
            if len(no_source_channels) > 10:
                print(f"    ... 还有 {len(no_source_channels) - 10} 个无源频道")

    def verify_cleanup(self) -> Dict[str, Any]:
        """验证资源是否完全清理"""
        verification = {
            'temp_dir_clean': False,
            'cache_size': 0,
            'backup_files': 0,
            'overall_clean': False
        }
        
        try:
            # 检查临时目录
            temp_dir = Path(self.config.temp_dir)
            if temp_dir.exists():
                temp_files = list(temp_dir.iterdir())
                verification['temp_dir_clean'] = len(temp_files) == 0
                verification['temp_files_remaining'] = len(temp_files)
            else:
                verification['temp_dir_clean'] = True
            
            # 检查缓存目录大小
            if self.cache_dir.exists():
                cache_files = list(self.cache_dir.glob("*.cache"))
                verification['cache_size'] = len(cache_files)
            
            # 检查备份文件
            if self.backup_dir.exists():
                backup_files = list(self.backup_dir.glob("backup_*.json"))
                verification['backup_files'] = len(backup_files)
            
            # 总体清理状态
            verification['overall_clean'] = (
                verification['temp_dir_clean'] and 
                verification['cache_size'] <= 50  # 允许一定数量的缓存文件
            )
            
            logger.info("✅ 资源清理验证完成")
            return verification
            
        except Exception as e:
            logger.error(f"❌ 资源清理验证失败: {e}")
            verification['error'] = str(e)
            return verification

    def cleanup(self):
        """完整的清理工作"""
        try:
            # 清理临时目录
            temp_dir = Path(self.config.temp_dir)
            if temp_dir.exists():
                temp_files_cleaned = 0
                for file in temp_dir.iterdir():
                    if file.is_file():
                        try:
                            file.unlink()
                            temp_files_cleaned += 1
                            logger.debug(f"删除临时文件: {file}")
                        except Exception as e:
                            logger.debug(f"删除临时文件失败: {file} - {e}")
                
                if temp_files_cleaned > 0:
                    logger.info(f"✅ 清理了 {temp_files_cleaned} 个临时文件")
            
            # 清理过期缓存
            if self.config.cache_enabled and self.cache_dir.exists():
                current_time = time.time()
                cache_files_cleaned = 0
                for cache_file in self.cache_dir.iterdir():
                    if cache_file.is_file() and cache_file.suffix == '.cache':
                        try:
                            with open(cache_file, 'r', encoding='utf-8') as f:
                                cache_data = json.load(f)
                            
                            cache_time = cache_data.get('timestamp', 0)
                            if current_time - cache_time > self.config.cache_expiry:
                                cache_file.unlink()
                                cache_files_cleaned += 1
                                logger.debug(f"删除过期缓存: {cache_file}")
                        except Exception as e:
                            logger.debug(f"处理缓存文件失败: {cache_file} - {e}")
                
                if cache_files_cleaned > 0:
                    logger.info(f"✅ 清理了 {cache_files_cleaned} 个过期缓存文件")
        
        except Exception as e:
            logger.error(f"清理过程中出错: {e}")

    def create_demo_template(self) -> bool:
        """创建示例模板文件"""
        demo_content = """# IPTV频道模板文件
# 格式: 分类名称,#genre#
#        频道名称1
#        频道名称2

央视频道,#genre#
CCTV-1 综合
CCTV-2 财经
CCTV-3 综艺
CCTV-4 中文国际
CCTV-5 体育
CCTV-6 电影
CCTV-7 国防军事
CCTV-8 电视剧
CCTV-9 纪录
CCTV-10 科教
CCTV-11 戏曲
CCTV-12 社会与法
CCTV-13 新闻
CCTV-14 少儿
CCTV-15 音乐

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

地方频道,#genre#
北京新闻
上海新闻
广州综合
重庆卫视
成都新闻

高清频道,#genre#
CCTV-1 HD
CCTV-5+ HD
湖南卫视 HD
浙江卫视 HD
"""
        try:
            with open(self.config.template_file, 'w', encoding='utf-8') as f:
                f.write(demo_content)
            logger.info(f"✅ 已创建示例模板文件: {Path(self.config.template_file).absolute()}")
            return True
        except Exception as e:
            logger.error(f"❌ 创建模板文件失败: {e}")
            return False

    def run(self):
        """完整的主运行函数"""
        print("=" * 60)
        print("🎬 IPTV智能管理工具 - 完整企业级版本 v8.0")
        print("=" * 60)
        
        # 检查依赖
        if not self.check_dependencies():
            print("❌ 依赖检查失败，程序退出")
            return
        
        # 验证配置
        if not self.validate_config():
            print("❌ 配置验证失败，程序退出")
            return
        
        # 保存初始配置
        if not self.save_config():
            print("⚠️  配置保存失败，但程序将继续运行")
        
        # 检查恢复点
        can_resume, checkpoint_data = self.can_resume_from_checkpoint()
        if can_resume:
            print(f"🔍 发现检查点: {checkpoint_data['stage']}")
            response = input("是否从检查点恢复? (y/N): ")
            if response.lower() == 'y':
                print("⏩ 从检查点恢复功能待实现，开始新的处理流程...")
            else:
                print("🔄 开始新的处理流程...")
        
        # 检查模板文件，如果不存在则创建示例
        template_path = Path(self.config.template_file)
        if not template_path.exists():
            print("📝 未找到模板文件，创建示例模板...")
            if self.create_demo_template():
                print(f"\n💡 模板文件已创建，请编辑以下文件后重新运行程序:")
                print(f"   📄 {template_path.absolute()}")
                print("\n模板内容包含常见的央视、卫视等频道")
                input("按回车键退出...")
            return
        
        start_time = time.time()
        
        try:
            # 1. 加载模板
            print("\n📋 步骤 1/7: 加载频道模板")
            self.save_checkpoint("loading_template")
            template_categories = self.load_template()
            if not template_categories:
                return
            
            # 2. 获取所有源数据
            print("\n🌐 步骤 2/7: 获取源数据")
            self.save_checkpoint("fetching_sources")
            content = self.fetch_all_streams()
            if not content:
                print("❌ 未能获取任何源数据")
                return
            
            # 备份原始数据
            self.backup_data("raw_content", content)
            
            # 3. 整理源数据
            print("\n🔧 步骤 3/7: 整理源数据")
            self.save_checkpoint("organizing_streams")
            sources_df = self.organize_streams(content)
            if sources_df.empty:
                print("❌ 未能解析出有效的流数据")
                return
            
            # 备份整理后的数据
            self.backup_data("organized_streams", sources_df)
            
            # 4. 获取所有模板频道
            all_template_channels = []
            for channels in template_categories.values():
                all_template_channels.extend(channels)
            
            # 5. 过滤和匹配频道
            print("\n🎯 步骤 4/7: 频道匹配")
            self.save_checkpoint("matching_channels")
            filtered_df = self.filter_and_sort_sources(sources_df, all_template_channels)
            if filtered_df.empty:
                print("❌ 没有匹配到任何模板频道")
                return
            
            # 备份匹配结果
            self.backup_data("matched_channels", filtered_df)
            
            # 6. 测速
            print("\n⚡ 步骤 5/7: 源测速")
            self.save_checkpoint("speed_testing")
            speed_tested_df = self.speed_test_sources(filtered_df)
            if speed_tested_df.empty:
                print("❌ 没有可用的源通过测速")
                return
            
            # 7. 生成最终数据
            print("\n🎨 步骤 6/7: 生成播放列表")
            self.save_checkpoint("generating_final_data")
            final_data = self.generate_final_data(speed_tested_df, template_categories)
            
            # 8. 保存文件
            print("\n💾 步骤 7/7: 保存文件")
            self.save_checkpoint("saving_files")
            if not self.save_output_files(final_data):
                print("❌ 文件保存失败")
                return
            
            # 9. 验证输出文件
            print("\n🔍 验证输出文件...")
            validation_result = self.validate_output_files()
            
            # 10. 生成完整性报告
            integrity_report = self.generate_integrity_report(final_data)
            
            # 11. 打印统计和报告
            self.print_statistics(final_data)
            
            # 打印验证结果
            print("\n📊 文件验证结果:")
            if validation_result['txt_file']['valid']:
                print(f"  ✅ TXT文件: {validation_result['txt_file']['categories']}分类, {validation_result['txt_file']['sources']}个源")
            else:
                print(f"  ❌ TXT文件: 无效或为空")
            
            if validation_result['m3u_file']['valid']:
                print(f"  ✅ M3U文件: {validation_result['m3u_file']['channels']}个频道, {validation_result['m3u_file']['sources']}个源")
            else:
                print(f"  ❌ M3U文件: 无效或格式错误")
            
            # 打印完整性报告摘要
            print(f"\n📈 数据完整性: {integrity_report['overall_status'].upper()}")
            print(f"📺 频道覆盖率: {integrity_report['channel_coverage']['coverage_rate']}%")
            print(f"🔗 平均源数: {integrity_report['channel_coverage']['avg_sources_per_channel']:.2f}")
            
            if integrity_report['recommendations']:
                print("\n💡 优化建议:")
                for recommendation in integrity_report['recommendations']:
                    print(f"  • {recommendation}")
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            print("\n🎉 处理完成!")
            print(f"⏰ 总耗时: {elapsed_time:.2f} 秒")
            print(f"📁 生成文件位置:")
            print(f"   📄 {Path(self.config.output_txt).absolute()}")
            print(f"   📄 {Path(self.config.output_m3u).absolute()}")
            
            # 清理检查点（成功完成）
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()
            
            # 验证资源清理
            cleanup_verification = self.verify_cleanup()
            if not cleanup_verification['overall_clean']:
                print(f"⚠️  资源清理警告: 还有{cleanup_verification['temp_files_remaining']}个临时文件未清理")
                
        except KeyboardInterrupt:
            print("\n⚠️  用户中断操作")
            print("💾 已保存检查点，下次可以恢复处理")
        except Exception as e:
            print(f"\n❌ 程序运行出错: {e}")
            import traceback
            traceback.print_exc()
            print("💾 错误检查点已保存")
        finally:
            # 清理临时文件
            self.cleanup()

def main():
    """主函数"""
    try:
        # 尝试加载现有配置
        manager = IPTVManager()
        config = manager.load_config()
        if config:
            manager = IPTVManager(config)
            print("✅ 使用已保存的配置")
        else:
            print("ℹ️  使用默认配置")
        
        manager.run()
    except Exception as e:
        print(f"❌ 程序启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
