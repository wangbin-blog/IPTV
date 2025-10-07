#!/usr/bin/env python3
"""
IPTV智能管理工具 - 优化测速版
功能：智能多源抓取、智能测速（关闭FFmpeg）、播放列表生成
版本：v3.2 (优化测速版)
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
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path
import shutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('IPTVManager')

class Config:
    """配置类 - 优化测速版"""
    
    # ==================== 文件配置（根目录） ====================
    TEMPLATE_FILE: str = "demo.txt"              # 模板文件（根目录）
    OUTPUT_TXT: str = "iptv.txt"                 # 输出TXT文件（根目录）
    OUTPUT_M3U: str = "iptv.m3u"                 # 输出M3U文件（根目录）
    TEMP_DIR: str = "temp"                       # 临时文件目录
    
    # ==================== 网络配置 ====================
    REQUEST_TIMEOUT: int = 20                    # 请求超时时间(秒)
    REQUEST_RETRIES: int = 3                     # 请求重试次数
    MAX_WORKERS: int = 5                         # 最大并发数
    
    # ==================== 测速配置 ====================
    SPEED_TEST_TIMEOUT: int = 8                  # HTTP测速超时时间(秒)
    FFMPEG_TEST_DURATION: int = 5                # FFmpeg测试时长(秒)
    FFMPEG_PROCESS_TIMEOUT: int = 12             # FFmpeg进程超时(秒)
    
    # ==================== 匹配配置 ====================
    SIMILARITY_THRESHOLD: int = 60               # 相似度阈值(0-100)
    MAX_SOURCES_PER_CHANNEL: int = 8             # 每个频道最大源数量
    
    # ==================== 智能源URL配置 ====================
    SOURCE_URLS: List[str] = [
        # 国内稳定源（优先）
            "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
            "https://raw.githubusercontent.com/iptv-org/iptv/gh-pages/countries/cn.m3u",
            "https://ghfast.top/raw.githubusercontent.com/Supprise0901/TVBox_live/main/live.txt",
            "https://gh-proxy.com/https://raw.githubusercontent.com/wwb521/live/main/tv.m3u",
            "https://gh-proxy.com/https://raw.githubusercontent.com/zeee-u/lzh06/main/fl.m3u",
            "https://raw.githubusercontent.com/Guovin/iptv-database/master/result.txt",  
            "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/cn.m3u",
            "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
            "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
            "http://47.120.41.246:8899/zb.txt",
            "https://live.zbds.top/tv/iptv4.txt",
    ]
    
    # ==================== 请求头配置 ====================
    HEADERS: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache'
    }
    
    # ==================== 流类型配置 ====================
    STREAM_TYPES: Dict[str, str] = {
        'm3u8': '.m3u8',
        'ts': '.ts',
        'flv': '.flv',
        'mp4': '.mp4',
        'rtmp': 'rtmp://',
        'rtsp': 'rtsp://'
    }
    
    # ==================== 测速模式配置 ====================
    USE_FFMPEG_TEST: bool = False  # 关闭FFmpeg测速，使用智能HTTP测速


class IPTVManager:
    """IPTV智能管理工具核心类 - 优化测速版"""
    
    def __init__(self, config: Config = None) -> None:
        """初始化IPTV管理器"""
        self.config: Config = config or Config()
        
        # 初始化会话
        self.session: requests.Session = requests.Session()
        self.session.headers.update(self.config.HEADERS)
        
        # 编译正则表达式
        self.patterns: Dict[str, re.Pattern] = {}
        self._compile_patterns()
        
        # 创建必要的目录
        self._setup_directories()
        
        # 检查FFmpeg（仅用于信息显示）
        self.ffmpeg_available: bool = self._check_ffmpeg()
        
        # 统计信息
        self.stats: Dict[str, int] = {
            'sources_fetched': 0,
            'streams_parsed': 0,
            'channels_matched': 0,
            'sources_tested': 0,
            'sources_available': 0
        }
        
        # 频道测速结果存储
        self.channel_speed_results: Dict[str, List[Dict]] = {}
        
        # 打印配置信息
        self._print_config()

    def _print_config(self) -> None:
        """打印配置信息"""
        logger.info("=" * 50)
        logger.info("🛠️ IPTV管理器配置信息")
        logger.info("=" * 50)
        logger.info(f"📁 模板文件: {self.config.TEMPLATE_FILE}")
        logger.info(f"📁 输出文件: {self.config.OUTPUT_TXT}, {self.config.OUTPUT_M3U}")
        logger.info(f"🌐 源数量: {len(self.config.SOURCE_URLS)}")
        logger.info(f"⚡ 并发数: {self.config.MAX_WORKERS}")
        logger.info(f"⏱️  测速超时: {self.config.SPEED_TEST_TIMEOUT}秒")
        logger.info(f"🎯 相似度阈值: {self.config.SIMILARITY_THRESHOLD}")
        logger.info(f"📺 每频道最大源: {self.config.MAX_SOURCES_PER_CHANNEL}")
        logger.info(f"🔧 FFmpeg测速: {'开启' if self.config.USE_FFMPEG_TEST else '关闭'}")
        logger.info("=" * 50)

    def _setup_directories(self) -> None:
        """设置必要的目录"""
        try:
            temp_path: Path = Path(self.config.TEMP_DIR)
            temp_path.mkdir(exist_ok=True)
            logger.info("✅ 目录初始化完成")
        except Exception as e:
            logger.error(f"❌ 目录设置失败: {e}")
            raise

    def _compile_patterns(self) -> None:
        """编译正则表达式模式"""
        try:
            self.patterns = {
                'extinf': re.compile(r'#EXTINF:.*?tvg-name="([^"]+)".*?,(.+)', re.IGNORECASE),
                'category': re.compile(r'^(.*?),#genre#$', re.IGNORECASE),
                'url': re.compile(r'https?://[^\s,]+', re.IGNORECASE),
                'tvg_name': re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE),
                'tvg_id': re.compile(r'tvg-id="([^"]*)"', re.IGNORECASE),
                'group_title': re.compile(r'group-title="([^"]*)"', re.IGNORECASE),
                'extinf_content': re.compile(r',\s*(.+)$', re.IGNORECASE),
                'channel_code': re.compile(r'([A-Z]+)-?(\d+)', re.IGNORECASE),
                'quality_suffix': re.compile(r'\s+(HD|FHD|4K|8K|高清|超清|直播|LIVE|频道|TV)', re.IGNORECASE),
                'brackets': re.compile(r'[\[\(\{].*?[\]\)\}]')
            }
            logger.debug("✅ 正则表达式编译完成")
        except Exception as e:
            logger.error(f"❌ 正则表达式编译失败: {e}")
            raise

    def _check_ffmpeg(self) -> bool:
        """检查FFmpeg是否可用（仅用于信息显示）"""
        if not self.config.USE_FFMPEG_TEST:
            return False
            
        try:
            result = subprocess.run(
                ['ffmpeg', '-version'], 
                capture_output=True, 
                timeout=5,
                check=False
            )
            available: bool = result.returncode == 0
            if available:
                logger.info("✅ FFmpeg可用 - 将使用智能流媒体测试")
            else:
                logger.warning("⚠️ FFmpeg未安装或不可用，将使用HTTP测速")
            return available
        except Exception as e:
            logger.warning(f"⚠️ FFmpeg检查失败: {e}，将使用HTTP测速")
            return False

    def validate_url(self, url: str) -> bool:
        """验证URL格式是否正确"""
        if not url or not isinstance(url, str):
            return False
            
        try:
            result = urlparse(url)
            valid_scheme: bool = result.scheme in ['http', 'https', 'rtmp', 'rtsp']
            valid_netloc: bool = bool(result.netloc)
            return all([valid_scheme, valid_netloc])
        except Exception:
            return False

    def fetch_streams_from_url(self, url: str) -> Optional[str]:
        """从URL获取流数据"""
        if not self.validate_url(url):
            logger.error(f"❌ 无效的URL: {url}")
            return None
            
        logger.info(f"📡 正在获取: {url}")
        
        for attempt in range(self.config.REQUEST_RETRIES):
            try:
                timeout: int = self.config.REQUEST_TIMEOUT + (attempt * 5)
                
                response: requests.Response = self.session.get(
                    url, 
                    timeout=timeout,
                    headers=self.config.HEADERS,
                    stream=True
                )
                response.encoding = 'utf-8'
                
                if response.status_code == 200:
                    content: str = response.text
                    content_length: int = len(content)
                    
                    if content_length < 10:
                        logger.warning(f"⚠️ 内容过短: {url} ({content_length} 字符)")
                        continue
                        
                    self.stats['sources_fetched'] += 1
                    logger.info(f"✅ 成功获取: {url} ({content_length} 字符)")
                    return content
                    
                elif response.status_code == 429:
                    wait_time: int = (attempt + 1) * 10
                    logger.warning(f"⚠️ 请求频繁，等待 {wait_time} 秒")
                    time.sleep(wait_time)
                    continue
                    
                elif response.status_code == 403:
                    logger.warning(f"⚠️ 访问被拒绝: {url}")
                    break
                    
                else:
                    logger.warning(f"⚠️ 获取失败，状态码: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ 请求超时，尝试 {attempt + 1}/{self.config.REQUEST_RETRIES}")
            except requests.exceptions.ConnectionError:
                logger.warning(f"⚠️ 连接错误，尝试 {attempt + 1}/{self.config.REQUEST_RETRIES}")
            except requests.exceptions.TooManyRedirects:
                logger.warning(f"⚠️ 重定向过多: {url}")
                break
            except Exception as e:
                logger.warning(f"⚠️ 请求异常: {e}")
                
            if attempt < self.config.REQUEST_RETRIES - 1:
                wait_time = (attempt + 1) * 3
                time.sleep(wait_time)
        
        logger.error(f"❌ 所有重试失败: {url}")
        return None

    def fetch_all_streams(self) -> str:
        """获取所有源的流数据"""
        logger.info("🚀 开始智能多源抓取...")
        
        if not self.config.SOURCE_URLS:
            logger.error("❌ 没有配置源URL")
            return ""
        
        all_streams: List[str] = []
        successful_sources: int = 0
        
        print("🌐 抓取进度: ", end="", flush=True)
        
        with ThreadPoolExecutor(max_workers=min(self.config.MAX_WORKERS, len(self.config.SOURCE_URLS))) as executor:
            future_to_url = {executor.submit(self.fetch_streams_from_url, url): url for url in self.config.SOURCE_URLS}
            
            for future in as_completed(future_to_url):
                url: str = future_to_url[future]
                try:
                    content: Optional[str] = future.result(timeout=self.config.REQUEST_TIMEOUT + 10)
                    if content:
                        all_streams.append(content)
                        successful_sources += 1
                        print("✅", end="", flush=True)
                    else:
                        print("❌", end="", flush=True)
                except Exception as e:
                    logger.error(f"处理 {url} 时发生错误: {e}")
                    print("💥", end="", flush=True)
        
        print()  # 换行
        logger.info(f"✅ 成功获取 {successful_sources}/{len(self.config.SOURCE_URLS)} 个源的数据")
        
        return "\n".join(all_streams) if all_streams else ""

    def _extract_program_name(self, extinf_line: str) -> str:
        """从EXTINF行提取节目名称"""
        if not extinf_line.startswith('#EXTINF'):
            return "未知频道"
        
        try:
            # 从tvg-name属性提取
            tvg_match = self.patterns['tvg_name'].search(extinf_line)
            if tvg_match and tvg_match.group(1).strip():
                name: str = tvg_match.group(1).strip()
                if name and name != "未知频道":
                    return name
            
            # 从逗号后的内容提取
            content_match = self.patterns['extinf_content'].search(extinf_line)
            if content_match and content_match.group(1).strip():
                name = content_match.group(1).strip()
                # 清理名称
                name = self.patterns['brackets'].sub('', name)
                name = self.patterns['quality_suffix'].sub('', name)
                name = name.strip()
                if name and name != "未知频道":
                    return name
                        
        except Exception as e:
            logger.debug(f"EXTINF解析错误: {extinf_line} - {e}")
        
        return "未知频道"

    def parse_m3u(self, content: str) -> List[Dict[str, str]]:
        """解析M3U格式内容"""
        if not content:
            return []
            
        streams: List[Dict[str, str]] = []
        lines: List[str] = content.splitlines()
        current_program: Optional[str] = None
        current_group: str = "默认分组"
        
        i: int = 0
        while i < len(lines):
            line: str = lines[i].strip()
            
            if not line:
                i += 1
                continue
                
            if line.startswith("#EXTINF"):
                current_program = self._extract_program_name(line)
                
                group_match = self.patterns['group_title'].search(line)
                if group_match:
                    current_group = group_match.group(1).strip() or "默认分组"
                else:
                    current_group = "默认分组"
                    
                # 查找下一个URL行
                j: int = i + 1
                while j < len(lines):
                    next_line: str = lines[j].strip()
                    if next_line and not next_line.startswith('#'):
                        if self.validate_url(next_line):
                            streams.append({
                                "program_name": current_program,
                                "stream_url": next_line,
                                "group": current_group
                            })
                        i = j  # 跳过URL行
                        break
                    j += 1
            elif line.startswith(('http://', 'https://', 'rtmp://', 'rtsp://')):
                if self.validate_url(line):
                    streams.append({
                        "program_name": "未知频道",
                        "stream_url": line,
                        "group": "默认分组"
                    })
            
            i += 1
        
        return streams

    def parse_txt(self, content: str) -> List[Dict[str, str]]:
        """解析TXT格式内容"""
        if not content:
            return []
            
        streams: List[Dict[str, str]] = []
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '#genre#' in line:
                continue
            
            # 支持多种分隔符
            separators = [',', ' ', '\t', '|']
            for sep in separators:
                if sep in line:
                    parts: List[str] = line.split(sep, 1)
                    if len(parts) == 2:
                        program_name: str = parts[0].strip()
                        url_part: str = parts[1].strip()
                        
                        url_match = self.patterns['url'].search(url_part)
                        if url_match:
                            stream_url: str = url_match.group()
                            if self.validate_url(stream_url):
                                streams.append({
                                    "program_name": program_name,
                                    "stream_url": stream_url,
                                    "group": "默认分组"
                                })
                                break
                    break
            else:
                # 没有分隔符，尝试直接提取URL
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
                            "group": "默认分组"
                        })
        
        return streams

    def organize_streams(self, content: str) -> pd.DataFrame:
        """整理流数据"""
        if not content:
            logger.error("❌ 没有内容可处理")
            return pd.DataFrame()
            
        logger.info("🔍 解析流数据...")
        
        try:
            # 自动检测格式
            if content.startswith("#EXTM3U"):
                streams: List[Dict[str, str]] = self.parse_m3u(content)
            else:
                streams = self.parse_txt(content)
            
            if not streams:
                logger.error("❌ 未能解析出任何流数据")
                return pd.DataFrame()
                
            df: pd.DataFrame = pd.DataFrame(streams)
            self.stats['streams_parsed'] = len(df)
            
            # 数据清理
            initial_count: int = len(df)
            
            # 移除空值和无效数据
            df = df.dropna()
            df = df[df['program_name'].str.len() > 0]
            df = df[df['stream_url'].str.len() > 0]
            
            # 验证URL
            df['url_valid'] = df['stream_url'].apply(self.validate_url)
            df = df[df['url_valid']].drop('url_valid', axis=1)
            
            # 去重
            df = df.drop_duplicates(subset=['program_name', 'stream_url'], keep='first')
            
            final_count: int = len(df)
            removed_count: int = initial_count - final_count
            
            logger.info(f"📊 数据清理完成: {initial_count} -> {final_count} 个流 (移除 {removed_count} 个无效数据)")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 数据处理错误: {e}")
            return pd.DataFrame()

    def load_template(self) -> Optional[Dict[str, List[str]]]:
        """加载频道模板文件"""
        template_file: Path = Path(self.config.TEMPLATE_FILE)
        
        if not template_file.exists():
            logger.error(f"❌ 模板文件 {template_file} 不存在")
            return None
            
        logger.info(f"📋 加载模板文件: {template_file}")
        categories: Dict[str, List[str]] = {}
        current_category: Optional[str] = None
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    category_match = self.patterns['category'].match(line)
                    if category_match:
                        current_category = category_match.group(1).strip()
                        categories[current_category] = []
                    
                    elif current_category and line and not line.startswith('#'):
                        channel_name: str = line.split(',')[0].strip() if ',' in line else line.strip()
                        if channel_name:
                            categories[current_category].append(channel_name)
        
        except Exception as e:
            logger.error(f"❌ 读取模板文件失败: {e}")
            return None
        
        if not categories:
            logger.error("❌ 模板文件中未找到有效的频道分类")
            return None
            
        total_channels: int = sum(len(channels) for channels in categories.values())
        logger.info(f"📁 模板分类: {list(categories.keys())}")
        logger.info(f"📺 模板频道总数: {total_channels}")
        
        return categories

    def clean_channel_name(self, name: str) -> str:
        """频道名称清理"""
        if not name:
            return ""
        
        try:
            cleaned: str = name.lower()
            
            # 移除质量标识
            cleaned = self.patterns['quality_suffix'].sub(' ', cleaned)
            
            # 移除括号内容
            cleaned = self.patterns['brackets'].sub('', cleaned)
            
            # 标准化频道代码
            code_match = self.patterns['channel_code'].search(cleaned)
            if code_match:
                prefix: str = code_match.group(1).upper()
                number: str = code_match.group(2)
                cleaned = f"{prefix} {number}"
            
            # 移除特殊字符，保留中文、英文、数字、空格
            cleaned = re.sub(r'[^\w\u4e00-\u9fa5\s-]', ' ', cleaned)
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            
            return cleaned
            
        except Exception as e:
            logger.debug(f"频道名称清理错误: {name} - {e}")
            return name.lower() if name else ""

    def similarity_score(self, str1: str, str2: str) -> int:
        """计算两个字符串的相似度分数（0-100）"""
        if not str1 or not str2:
            return 0
            
        try:
            clean_str1: str = self.clean_channel_name(str1)
            clean_str2: str = self.clean_channel_name(str2)
            
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
            
            # 编辑距离相似度
            def edit_distance_similarity(s1: str, s2: str) -> float:
                if len(s1) > len(s2):
                    s1, s2 = s2, s1
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
                if max_len == 0:
                    return 100.0
                return (1 - distances[-1] / max_len) * 100
            
            edit_score: float = edit_distance_similarity(clean_str1, clean_str2)
            
            # Jaccard相似度
            set1 = set(clean_str1)
            set2 = set(clean_str2)
            
            intersection = len(set1 & set2)
            union = len(set1 | set2)
            
            jaccard_similarity: float = (intersection / union) * 100 if union > 0 else 0
            
            # 综合评分（加权平均）
            final_score: float = (edit_score * 0.6 + jaccard_similarity * 0.4)
            
            return int(final_score)
                
        except Exception as e:
            logger.debug(f"相似度计算错误: {str1}, {str2} - {e}")
        
        return 0

    def filter_and_sort_sources(self, sources_df: pd.DataFrame, template_channels: List[str]) -> pd.DataFrame:
        """频道匹配和源筛选"""
        logger.info("🎯 开始智能频道匹配...")
        
        if sources_df.empty or not template_channels:
            logger.error("❌ 源数据或模板频道为空")
            return pd.DataFrame()
        
        matched_results: List[Dict[str, Any]] = []
        match_stats: Dict[str, int] = {'exact': 0, 'good': 0, 'fair': 0}
        
        print("🔍 匹配进度: ", end="", flush=True)
        
        for template_channel in template_channels:
            best_match_row = None
            best_score: int = 0
            best_original_name: str = ""
            
            for _, source_row in sources_df.iterrows():
                source_channel: str = source_row['program_name']
                score: int = self.similarity_score(template_channel, source_channel)
                
                if score > best_score and score >= self.config.SIMILARITY_THRESHOLD:
                    best_score = score
                    best_match_row = source_row.copy()
                    best_original_name = source_channel
            
            if best_match_row is not None:
                best_match_row['template_channel'] = template_channel
                best_match_row['match_score'] = best_score
                best_match_row['original_name'] = best_original_name
                
                matched_results.append(best_match_row)
                
                # 统计匹配质量
                if best_score >= 90:
                    match_stats['exact'] += 1
                    print("🎯", end="", flush=True)
                elif best_score >= 70:
                    match_stats['good'] += 1
                    print("✅", end="", flush=True)
                else:
                    match_stats['fair'] += 1
                    print("👍", end="", flush=True)
            else:
                print("❌", end="", flush=True)
        
        print()  # 换行
        
        if matched_results:
            result_df: pd.DataFrame = pd.DataFrame(matched_results)
            result_df = result_df.rename(columns={'program_name': 'original_name'})
            result_df = result_df.rename(columns={'template_channel': 'program_name'})
            
            # 按匹配分数排序
            result_df = result_df.sort_values(['program_name', 'match_score'], ascending=[True, False])
            
            unique_matched_channels: int = result_df['program_name'].nunique()
            self.stats['channels_matched'] = unique_matched_channels
            
            logger.info(f"✅ 频道匹配完成: {len(matched_results)} 个流匹配到 {unique_matched_channels} 个模板频道")
            logger.info(f"📊 匹配质量: 精确{match_stats['exact']} 良好{match_stats['good']} 一般{match_stats['fair']}")
            
            return result_df
        else:
            logger.error("❌ 没有找到任何匹配的频道")
            return pd.DataFrame()

    def speed_test_simple(self, stream_url: str) -> Tuple[bool, float]:
        """智能HTTP测速"""
        if not stream_url:
            return False, float('inf')
            
        try:
            start_time: float = time.time()
            response: requests.Response = self.session.head(
                stream_url, 
                timeout=self.config.SPEED_TEST_TIMEOUT,
                allow_redirects=True,
                headers={
                    **self.config.HEADERS,
                    'Range': 'bytes=0-1'
                }
            )
            end_time: float = time.time()
            
            if response.status_code in [200, 206, 302, 301, 307]:
                content_type: str = response.headers.get('Content-Type', '').lower()
                content_length: str = response.headers.get('Content-Length', '')
                
                # 更智能的内容类型判断
                valid_content_types = ['video/', 'audio/', 'application/', 'text/', 'image/']
                valid_content = any(ct in content_type for ct in valid_content_types)
                
                # 检查是否是有效的流媒体
                is_stream: bool = (
                    'm3u' in content_type or 
                    'm3u' in stream_url.lower() or
                    content_type.startswith('video/') or
                    content_type.startswith('audio/') or
                    int(content_length) > 100 if content_length.isdigit() else False
                )
                
                if valid_content and is_stream:
                    speed: float = end_time - start_time
                    return True, speed
                else:
                    logger.debug(f"⚠️ 无效Content-Type或内容: {content_type} - {stream_url[:50]}...")
                    return False, float('inf')
            else:
                logger.debug(f"❌ HTTP状态码 {response.status_code}: {stream_url[:50]}...")
                return False, float('inf')
                
        except requests.exceptions.Timeout:
            logger.debug(f"⏰ HTTP测速超时: {stream_url[:50]}...")
            return False, float('inf')
        except Exception as e:
            logger.debug(f"⚠️ HTTP测速异常: {e} - {stream_url[:50]}...")
            return False, float('inf')

    def speed_test_sources(self, sources_df: pd.DataFrame) -> pd.DataFrame:
        """测速实现 - 显示每个频道结果"""
        logger.info(f"⏱️  开始智能测速 (HTTP模式)...")
        
        if sources_df.empty:
            logger.error("❌ 没有需要测速的源")
            return pd.DataFrame()
            
        results: List[Dict[str, Any]] = []
        total_sources: int = len(sources_df)
        
        print("\n⚡ 频道测速结果:")
        print("-" * 80)
        
        def test_single_source(row: pd.Series) -> Dict[str, Any]:
            try:
                program_name: str = row['program_name']
                stream_url: str = row['stream_url']
                original_name: str = row.get('original_name', '')
                match_score: int = row.get('match_score', 0)
                
                # 使用智能HTTP测速
                accessible, speed = self.speed_test_simple(stream_url)
                
                result = {
                    'program_name': program_name,
                    'stream_url': stream_url,
                    'accessible': accessible,
                    'speed': speed,
                    'original_name': original_name,
                    'match_score': match_score,
                    'stream_type': self._detect_stream_type(stream_url)
                }
                
                # 实时显示每个源的测速结果
                status_icon = "✅" if accessible else "❌"
                speed_display = f"{speed:.2f}s" if accessible else "超时"
                match_display = f"(匹配:{match_score}%)"
                
                print(f"  {status_icon} {program_name:20} {speed_display:8} {match_display:12} {original_name[:30]}...")
                
                return result
                
            except Exception as e:
                logger.debug(f"测速过程异常: {e}")
                print(f"  💥 {program_name:20} 错误        {original_name[:30]}...")
                return {
                    'program_name': row.get('program_name', '未知'),
                    'stream_url': row.get('stream_url', ''),
                    'accessible': False,
                    'speed': float('inf'),
                    'stream_type': 'error'
                }
        
        with ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
            futures = [executor.submit(test_single_source, row) for _, row in sources_df.iterrows()]
            
            completed: int = 0
            for future in as_completed(futures):
                try:
                    timeout: int = self.config.SPEED_TEST_TIMEOUT + 5
                    result: Dict[str, Any] = future.result(timeout=timeout)
                    results.append(result)
                    completed += 1
                        
                except TimeoutError:
                    print(f"  ⏰ 超时频道")
                    results.append({
                        'program_name': '超时频道',
                        'stream_url': '',
                        'accessible': False,
                        'speed': float('inf'),
                        'stream_type': 'timeout'
                    })
                except Exception as e:
                    print(f"  💥 测速异常")
                    logger.debug(f"测速任务异常: {e}")
        
        print("-" * 80)
        
        try:
            result_df: pd.DataFrame = pd.DataFrame(results)
            accessible_df: pd.DataFrame = result_df[result_df['accessible']].copy()
            
            if not accessible_df.empty:
                # 按速度和匹配分数综合排序
                accessible_df['composite_score'] = (
                    (1 / accessible_df['speed'].clip(lower=0.1)) * 0.7 + 
                    (accessible_df['match_score'] / 100) * 0.3
                )
                accessible_df = accessible_df.sort_values(['program_name', 'composite_score'], ascending=[True, False])
                accessible_df = accessible_df.drop('composite_score', axis=1)
            
            accessible_count: int = len(accessible_df)
            avg_speed: float = accessible_df['speed'].mean() if not accessible_df.empty else 0
            
            self.stats['sources_tested'] = total_sources
            self.stats['sources_available'] = accessible_count
            
            logger.info(f"📊 测速完成: {accessible_count}/{total_sources} 个源可用")
            logger.info(f"📈 平均响应时间: {avg_speed:.2f} 秒")
            
            # 存储频道测速结果用于后续显示
            for _, row in accessible_df.iterrows():
                channel = row['program_name']
                if channel not in self.channel_speed_results:
                    self.channel_speed_results[channel] = []
                self.channel_speed_results[channel].append({
                    'url': row['stream_url'],
                    'speed': row['speed'],
                    'match_score': row['match_score']
                })
            
            return accessible_df
            
        except Exception as e:
            logger.error(f"❌ 处理测速结果时出错: {e}")
            return pd.DataFrame()

    def _detect_stream_type(self, stream_url: str) -> str:
        """检测流媒体类型"""
        stream_url_lower: str = stream_url.lower()
        
        for stream_type, identifier in self.config.STREAM_TYPES.items():
            if identifier in stream_url_lower:
                return stream_type
        
        return 'unknown'

    def generate_final_data(self, speed_tested_df: pd.DataFrame, template_categories: Dict[str, List[str]]) -> Dict[str, Any]:
        """生成最终数据"""
        logger.info("🎨 生成最终播放列表...")
        
        final_data: Dict[str, Any] = {}
        total_sources: int = 0
        
        if speed_tested_df.empty or not template_categories:
            logger.error("❌ 测速数据或模板分类为空")
            return final_data
        
        print("\n📦 频道源统计:")
        print("-" * 60)
        
        for category, channels in template_categories.items():
            final_data[category] = {}
            
            for channel in channels:
                channel_sources = speed_tested_df[speed_tested_df['program_name'] == channel]
                
                if not channel_sources.empty:
                    # 每个频道选择最多8个最佳源
                    best_sources = channel_sources.head(self.config.MAX_SOURCES_PER_CHANNEL)
                    final_data[category][channel] = best_sources[['stream_url', 'speed', 'match_score']].to_dict('records')
                    total_sources += len(best_sources)
                    
                    source_count: int = len(best_sources)
                    speed_avg: float = sum(s['speed'] for s in final_data[category][channel]) / source_count
                    
                    # 显示每个频道的源数量和质量
                    quality_icon = "🚀" if speed_avg < 3 else "⚡" if speed_avg < 6 else "✅"
                    print(f"  {quality_icon} {channel:20} {source_count:2d}个源 平均{speed_avg:.2f}秒")
                    
                else:
                    final_data[category][channel] = []
                    print(f"  ❌ {channel:20} 0个源")
        
        print("-" * 60)
        logger.info(f"📦 总共收集到 {total_sources} 个有效源")
        
        return final_data

    def save_output_files(self, final_data: Dict[str, Any]) -> bool:
        """保存输出文件到根目录"""
        logger.info("💾 保存文件到根目录...")
        
        if not final_data:
            logger.error("❌ 没有数据需要保存")
            return False
        
        success_count: int = 0
        
        # 保存TXT格式
        try:
            with open(self.config.OUTPUT_TXT, 'w', encoding='utf-8') as f:
                f.write("# IPTV播放列表 - 生成时间: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
                f.write("# 每个频道提供多个备用源，最多8个\n")
                f.write("# 格式: 频道名称,直播流地址\n\n")
                
                for category, channels in final_data.items():
                    f.write(f"{category},#genre#\n")
                    
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f"{channel},{source['stream_url']}\n")
                    
                    f.write("\n")
            
            success_count += 1
            file_size: int = os.path.getsize(self.config.OUTPUT_TXT)
            logger.info(f"✅ TXT文件已保存: {self.config.OUTPUT_TXT} ({file_size} 字节)")
            
        except Exception as e:
            logger.error(f"❌ 保存TXT文件失败: {e}")
        
        # 保存M3U格式
        try:
            with open(self.config.OUTPUT_M3U, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                f.write("#PLAYLIST: IPTV智能列表\n")
                f.write("#GENERATED: " + time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
                f.write("#SOURCE: 多源智能聚合\n")
                f.write("#SPEED_TEST: HTTP智能测速\n")
                
                for category, channels in final_data.items():
                    for channel, sources in channels.items():
                        for idx, source in enumerate(sources, 1):
                            speed_info = f"响应{source['speed']:.1f}秒" if source['speed'] < 10 else "响应较慢"
                            display_name: str = f"{channel}" if len(sources) == 1 else f"{channel} [源{idx}-{speed_info}]"
                            f.write(f'#EXTINF:-1 tvg-name="{channel}" group-title="{category}",{display_name}\n')
                            f.write(f"{source['stream_url']}\n")
            
            success_count += 1
            file_size = os.path.getsize(self.config.OUTPUT_M3U)
            logger.info(f"✅ M3U文件已保存: {self.config.OUTPUT_M3U} ({file_size} 字节)")
            
        except Exception as e:
            logger.error(f"❌ 保存M3U文件失败: {e}")
            
        return success_count == 2

    def create_demo_template(self) -> bool:
        """创建示例模板文件到根目录"""
        demo_content: str = """# IPTV频道模板文件
# 格式: 分类名称,#genre#
#       频道名称1
#       频道名称2

央视频道,#genre#
CCTV-1
CCTV-2
CCTV-3
CCTV-4
CCTV-5
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
北京新闻
上海新闻
广州综合
重庆卫视
成都新闻
深圳新闻
杭州综合

高清频道,#genre#
CCTV-1 HD
CCTV-5 HD
湖南卫视 HD
浙江卫视 HD
江苏卫视 HD

影视频道,#genre#
CCTV-6
CCTV-8
湖南卫视电影
浙江卫视影视
"""
        try:
            with open(self.config.TEMPLATE_FILE, 'w', encoding='utf-8') as f:
                f.write(demo_content)
            logger.info(f"✅ 已创建示例模板文件: {self.config.TEMPLATE_FILE}")
            return True
        except Exception as e:
            logger.error(f"❌ 创建模板文件失败: {e}")
            return False

    def print_statistics(self, final_data: Dict[str, Any]) -> None:
        """打印详细统计信息"""
        print("\n" + "="*60)
        print("📈 详细统计报告")
        print("="*60)
        
        if not final_data:
            print("❌ 没有数据可统计")
            return
        
        total_channels: int = 0
        total_sources: int = 0
        category_details: List[Tuple[str, int, int]] = []
        
        for category, channels in final_data.items():
            category_channels: int = 0
            category_sources: int = 0
            
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
        
        for category, channel_count, source_count in category_details:
            avg_sources: float = source_count / channel_count if channel_count > 0 else 0
            print(f"  📺 {category}: {channel_count:2d}频道, {source_count:3d}源 (平均{avg_sources:.1f}源/频道)")
        
        print("-"*60)
        print(f"📊 总计: {total_channels}频道, {total_sources}源")
        print(f"🎯 配置: 每个频道最多{self.config.MAX_SOURCES_PER_CHANNEL}个源")
        
        # 显示处理统计
        print("-"*60)
        print(f"🌐 源抓取: {self.stats['sources_fetched']}个成功")
        print(f"🔧 流解析: {self.stats['streams_parsed']}个流")
        print(f"🎯 频道匹配: {self.stats['channels_matched']}个频道")
        print(f"⚡ 源测速: {self.stats['sources_tested']}个测试, {self.stats['sources_available']}个可用")
        print(f"🔧 测速模式: HTTP智能测速")

    def cleanup(self) -> None:
        """清理临时文件"""
        try:
            temp_dir: Path = Path(self.config.TEMP_DIR)
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                logger.info("✅ 临时文件清理完成")
        except Exception as e:
            logger.debug(f"清理临时文件时出错: {e}")

    def run(self) -> None:
        """主运行函数"""
        print("=" * 60)
        print("🎬 IPTV智能管理工具 - 优化测速版 v3.2")
        print("🔧 关闭FFmpeg测速 + 智能HTTP测速 + 详细结果显示")
        print("📺 每个频道最多8个备用源")
        print("=" * 60)
        
        start_time: float = time.time()
        
        try:
            # 检查模板文件
            template_path: Path = Path(self.config.TEMPLATE_FILE)
            if not template_path.exists():
                print("📝 未找到模板文件，创建示例模板...")
                if self.create_demo_template():
                    print(f"\n💡 模板文件已创建，请编辑后重新运行:")
                    print(f"   📄 {template_path.absolute()}")
                    input("按回车键退出...")
                return
            
            # 执行处理流程
            template_categories = self.load_template()
            if not template_categories:
                return
            
            content = self.fetch_all_streams()
            if not content:
                return
            
            sources_df = self.organize_streams(content)
            if sources_df.empty:
                return
            
            all_template_channels = []
            for channels in template_categories.values():
                all_template_channels.extend(channels)
            
            filtered_df = self.filter_and_sort_sources(sources_df, all_template_channels)
            if filtered_df.empty:
                return
            
            speed_tested_df = self.speed_test_sources(filtered_df)
            if speed_tested_df.empty:
                return
            
            final_data = self.generate_final_data(speed_tested_df, template_categories)
            
            # 保存文件
            if not self.save_output_files(final_data):
                return
            
            # 打印统计
            self.print_statistics(final_data)
            
            end_time: float = time.time()
            elapsed_time: float = end_time - start_time
            
            print("\n🎉 处理完成!")
            print(f"⏰ 总耗时: {elapsed_time:.2f} 秒")
            print(f"📁 生成文件 (根目录):")
            print(f"   📄 {Path(self.config.OUTPUT_TXT).absolute()}")
            print(f"   📄 {Path(self.config.OUTPUT_M3U).absolute()}")
                
        except KeyboardInterrupt:
            print("\n⚠️  用户中断操作")
        except Exception as e:
            print(f"\n❌ 程序运行出错: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()


def main() -> None:
    """主函数"""
    try:
        config = Config()
        manager = IPTVManager(config)
        manager.run()
    except Exception as e:
        print(f"❌ 程序启动失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
