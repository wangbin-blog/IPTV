#!/usr/bin/env python3
"""
IPTV智能管理工具 - 核心功能版本 (修复完整版)
功能：多源抓取、频道匹配、速度测试、播放列表生成
版本：v2.2 (智能测速优化版)
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
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('IPTVManager')

class IPTVManager:
    """IPTV智能管理工具核心类"""
    
    def __init__(self):
        """初始化IPTV管理器"""
        # 配置参数
        self.source_urls = [
            "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
            "https://live.zbds.top/tv/iptv6.txt", 
            "https://live.zbds.top/tv/iptv4.txt",
            "http://home.jundie.top:81/top/tvbox.txt",
            "https://mirror.ghproxy.com/https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
        ]
        self.request_timeout = 15
        self.max_sources_per_channel = 5
        self.speed_test_timeout = 10  # 统一超时时间为10秒
        self.similarity_threshold = 50
        self.max_workers = 3  # 减少并发数避免资源竞争
        self.template_file = "demo.txt"
        self.output_txt = "iptv.txt"
        self.output_m3u = "iptv.m3u"
        self.temp_dir = "temp"
        
        # 初始化会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # 创建必要的目录
        self._setup_directories()
        
        # 编译正则表达式
        self._compile_patterns()
        
        # 检查FFmpeg
        self.ffmpeg_available = self._check_ffmpeg()

    def _setup_directories(self) -> None:
        """设置必要的目录"""
        try:
            temp_path = Path(self.temp_dir)
            temp_path.mkdir(exist_ok=True)
            logger.info("✅ 目录初始化完成")
        except Exception as e:
            logger.error(f"❌ 目录设置失败: {e}")
            raise

    def _compile_patterns(self) -> None:
        """编译正则表达式模式"""
        self.patterns = {
            'extinf': re.compile(r'#EXTINF:.*?tvg-name="([^"]+)".*?,(.+)'),
            'category': re.compile(r'^(.*?),#genre#$'),
            'url': re.compile(r'https?://[^\s,]+'),
            'tvg_name': re.compile(r'tvg-name="([^"]*)"'),
            'tvg_id': re.compile(r'tvg-id="([^"]*)"'),
            'group_title': re.compile(r'group-title="([^"]*)"'),
            'extinf_content': re.compile(r',\s*(.+)$')
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
            if result.returncode == 0:
                logger.info("✅ FFmpeg可用")
                return True
            else:
                logger.warning("⚠️ FFmpeg未安装，将使用HTTP测速")
                return False
        except:
            logger.warning("⚠️ FFmpeg未安装，将使用HTTP测速")
            return False

    def validate_url(self, url: str) -> bool:
        """验证URL格式是否正确"""
        if not url or not isinstance(url, str):
            return False
            
        try:
            result = urlparse(url)
            valid_scheme = result.scheme in ['http', 'https']
            valid_netloc = bool(result.netloc)
            return all([valid_scheme, valid_netloc])
        except:
            return False

    def fetch_streams_from_url(self, url: str, retries: int = 2) -> Optional[str]:
        """从URL获取流数据"""
        if not self.validate_url(url):
            logger.error(f"❌ 无效的URL: {url}")
            return None
            
        logger.info(f"📡 正在获取: {url}")
        
        for attempt in range(retries):
            try:
                response = self.session.get(
                    url, 
                    timeout=self.request_timeout,
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                )
                response.encoding = 'utf-8'
                
                if response.status_code == 200:
                    content = response.text
                    content_length = len(content)
                    logger.info(f"✅ 成功获取: {url} ({content_length} 字符)")
                    return content
                    
                elif response.status_code == 429:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"⚠️ 请求频繁，等待 {wait_time} 秒")
                    time.sleep(wait_time)
                    continue
                    
                else:
                    logger.warning(f"⚠️ 获取失败，状态码: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⚠️ 请求超时，尝试 {attempt + 1}/{retries}")
            except requests.exceptions.ConnectionError:
                logger.warning(f"⚠️ 连接错误，尝试 {attempt + 1}/{retries}")
            except Exception as e:
                logger.warning(f"⚠️ 请求异常: {e}")
                
            if attempt < retries - 1:
                time.sleep(2)
        
        logger.error(f"❌ 所有重试失败: {url}")
        return None

    def fetch_all_streams(self) -> str:
        """获取所有源的流数据"""
        logger.info("🚀 开始多源抓取...")
        
        if not self.source_urls:
            logger.error("❌ 没有配置源URL")
            return ""
        
        all_streams = []
        successful_sources = 0
        
        print("🌐 抓取进度: ", end="")
        
        with ThreadPoolExecutor(max_workers=min(3, len(self.source_urls))) as executor:
            future_to_url = {executor.submit(self.fetch_streams_from_url, url): url for url in self.source_urls}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    content = future.result()
                    if content:
                        all_streams.append(content)
                        successful_sources += 1
                        print("✅", end="")
                    else:
                        print("❌", end="")
                except Exception as e:
                    logger.error(f"处理 {url} 时发生错误: {e}")
                    print("❌", end="")
        
        print()  # 换行
        logger.info(f"✅ 成功获取 {successful_sources}/{len(self.source_urls)} 个源的数据")
        return "\n".join(all_streams) if all_streams else ""

    def _extract_program_name(self, extinf_line: str) -> str:
        """从EXTINF行提取节目名称"""
        if not extinf_line.startswith('#EXTINF'):
            return "未知频道"
        
        try:
            # 从tvg-name属性提取
            tvg_match = self.patterns['tvg_name'].search(extinf_line)
            if tvg_match and tvg_match.group(1).strip():
                name = tvg_match.group(1).strip()
                if name and name != "未知频道":
                    return name
            
            # 从逗号后的内容提取
            content_match = self.patterns['extinf_content'].search(extinf_line)
            if content_match and content_match.group(1).strip():
                name = content_match.group(1).strip()
                name = re.sub(r'\[.*?\]|\(.*?\)', '', name).strip()
                if name and name != "未知频道":
                    return name
                        
        except Exception as e:
            logger.debug(f"EXTINF解析错误: {extinf_line} - {e}")
        
        return "未知频道"

    def parse_m3u(self, content: str) -> List[Dict[str, str]]:
        """解析M3U格式内容"""
        if not content:
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
                current_program = self._extract_program_name(line)
                
                group_match = self.patterns['group_title'].search(line)
                if group_match:
                    current_group = group_match.group(1).strip()
                else:
                    current_group = "默认分组"
                    
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if self.validate_url(next_line):
                        streams.append({
                            "program_name": current_program,
                            "stream_url": next_line,
                            "group": current_group
                        })
                        i += 1
            elif line.startswith(('http://', 'https://')):
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
            
        streams = []
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '#genre#' in line:
                continue
            
            if ',' in line:
                parts = line.split(',', 1)
                if len(parts) == 2:
                    program_name = parts[0].strip()
                    url_part = parts[1].strip()
                    
                    url_match = self.patterns['url'].search(url_part)
                    if url_match:
                        stream_url = url_match.group()
                        if self.validate_url(stream_url):
                            streams.append({
                                "program_name": program_name,
                                "stream_url": stream_url,
                                "group": "默认分组"
                            })
            else:
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
        """整理流数据，去除重复和无效数据"""
        if not content:
            logger.error("❌ 没有内容可处理")
            return pd.DataFrame()
            
        logger.info("🔍 解析流数据...")
        
        try:
            if content.startswith("#EXTM3U"):
                streams = self.parse_m3u(content)
            else:
                streams = self.parse_txt(content)
            
            if not streams:
                logger.error("❌ 未能解析出任何流数据")
                return pd.DataFrame()
                
            df = pd.DataFrame(streams)
            
            # 数据清理
            initial_count = len(df)
            
            # 移除空值和无效数据
            df = df.dropna()
            df = df[df['program_name'].str.len() > 0]
            df = df[df['stream_url'].str.startswith(('http://', 'https://'))]
            
            # 验证URL
            df['url_valid'] = df['stream_url'].apply(self.validate_url)
            df = df[df['url_valid']].drop('url_valid', axis=1)
            
            # 去重
            df = df.drop_duplicates(subset=['program_name', 'stream_url'])
            
            final_count = len(df)
            logger.info(f"📊 数据清理: {initial_count} -> {final_count} 个流")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 数据处理错误: {e}")
            return pd.DataFrame()

    def load_template(self) -> Optional[Dict[str, List[str]]]:
        """加载频道模板文件"""
        template_file = Path(self.template_file)
        
        if not template_file.exists():
            logger.error(f"❌ 模板文件 {template_file} 不存在")
            return None
            
        logger.info(f"📋 加载模板文件: {template_file}")
        categories = {}
        current_category = None
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    category_match = self.patterns['category'].match(line)
                    if category_match:
                        current_category = category_match.group(1).strip()
                        categories[current_category] = []
                    
                    elif current_category and line and not line.startswith('#'):
                        channel_name = line.split(',')[0].strip() if ',' in line else line.strip()
                        if channel_name:
                            categories[current_category].append(channel_name)
        
        except Exception as e:
            logger.error(f"❌ 读取模板文件失败: {e}")
            return None
        
        if not categories:
            logger.error("❌ 模板文件中未找到有效的频道分类")
            return None
            
        total_channels = sum(len(channels) for channels in categories.values())
        logger.info(f"📁 模板分类: {list(categories.keys())}")
        logger.info(f"📺 模板频道总数: {total_channels}")
        
        return categories

    def clean_channel_name(self, name: str) -> str:
        """频道名称清理"""
        if not name:
            return ""
        
        try:
            cleaned = re.sub(r'[^\w\u4e00-\u9fa5\s-]', '', name.lower())
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            cleaned = re.sub(r'\s+(hd|fhd|4k|直播|频道|tv|television)$', '', cleaned)
            return cleaned
        except:
            return name.lower() if name else ""

    def similarity_score(self, str1: str, str2: str) -> int:
        """计算两个字符串的相似度分数（0-100）"""
        if not str1 or not str2:
            return 0
            
        try:
            clean_str1 = self.clean_channel_name(str1)
            clean_str2 = self.clean_channel_name(str2)
            
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
            
            # Jaccard相似度
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
        """频道匹配和源筛选"""
        logger.info("🎯 开始频道匹配...")
        
        if sources_df.empty or not template_channels:
            logger.error("❌ 源数据或模板频道为空")
            return pd.DataFrame()
        
        matched_results = []
        
        print("🔍 匹配进度: ", end="")
        
        for template_channel in template_channels:
            best_match_row = None
            best_score = 0
            
            for _, source_row in sources_df.iterrows():
                source_channel = source_row['program_name']
                score = self.similarity_score(template_channel, source_channel)
                
                if score > best_score and score >= self.similarity_threshold:
                    best_score = score
                    best_match_row = source_row.copy()
                    best_match_row['template_channel'] = template_channel
                    best_match_row['match_score'] = score
            
            if best_match_row is not None:
                matched_results.append(best_match_row)
                print("✅", end="")
            else:
                print("❌", end="")
        
        print()  # 换行
        
        if matched_results:
            result_df = pd.DataFrame(matched_results)
            result_df = result_df.rename(columns={'program_name': 'original_name'})
            result_df = result_df.rename(columns={'template_channel': 'program_name'})
            
            unique_matched_channels = result_df['program_name'].nunique()
            logger.info(f"✅ 频道匹配完成: {len(matched_results)} 个流匹配到 {unique_matched_channels} 个模板频道")
            
            return result_df
        else:
            logger.error("❌ 没有找到任何匹配的频道")
            return pd.DataFrame()

    def speed_test_ffmpeg(self, stream_url: str) -> Tuple[bool, float]:
        """使用FFmpeg进行流媒体测速 - 10秒响应10秒超时"""
        if not self.ffmpeg_available or not stream_url:
            return False, float('inf')
            
        temp_file = Path(self.temp_dir) / f'test_{abs(hash(stream_url))}.ts'
        
        try:
            cmd = [
                'ffmpeg',
                '-y',
                '-timeout', '10000000',  # 10秒超时（微秒）
                '-rw_timeout', '10000000',  # 读写超时10秒
                '-i', stream_url,
                '-t', '10',  # 测试10秒内容
                '-c', 'copy',
                '-f', 'mpegts',
                '-max_muxing_queue_size', '1024',  # 增加队列大小
                str(temp_file)
            ]
            
            start_time = time.time()
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=15,  # 总进程超时15秒
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
                logger.info(f"✅ FFmpeg测速成功: {speed:.2f}秒 - {stream_url[:50]}...")
                return True, speed
            else:
                logger.debug(f"❌ FFmpeg测速失败: {result.stderr[:100]}...")
                return False, float('inf')
                
        except subprocess.TimeoutExpired:
            logger.debug(f"⏰ FFmpeg测速超时: {stream_url[:50]}...")
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            return False, float('inf')
        except Exception as e:
            logger.debug(f"⚠️ FFmpeg测速异常: {e} - {stream_url[:50]}...")
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
            return False, float('inf')

    def speed_test_simple(self, stream_url: str) -> Tuple[bool, float]:
        """简单的HTTP测速 - 10秒超时"""
        if not stream_url:
            return False, float('inf')
            
        try:
            start_time = time.time()
            response = self.session.head(
                stream_url, 
                timeout=10,  # 10秒超时
                allow_redirects=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': '*/*',
                    'Connection': 'close'
                }
            )
            end_time = time.time()
            
            if response.status_code in [200, 302, 301, 307]:
                speed = end_time - start_time
                logger.info(f"✅ HTTP测速成功: {speed:.2f}秒 - {stream_url[:50]}...")
                return True, speed
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
        """测速实现 - 优化超时处理"""
        logger.info("⏱️  开始测速 (FFmpeg:10秒测试+10秒超时, HTTP:10秒超时)...")
        
        if sources_df.empty:
            logger.error("❌ 没有需要测速的源")
            return pd.DataFrame()
            
        results = []
        total_sources = len(sources_df)
        
        print("⚡ 测速进度: ", end="")
        
        def test_single_source(row):
            try:
                program_name = row['program_name']
                stream_url = row['stream_url']
                
                # 根据流类型选择测速方式
                if any(ext in stream_url.lower() for ext in ['.m3u8', '.ts', '.flv', '.mp4', 'rtmp', 'rtsp']):
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
                logger.debug(f"测速过程异常: {e}")
                return {
                    'program_name': row.get('program_name', '未知'),
                    'stream_url': row.get('stream_url', ''),
                    'accessible': False,
                    'speed': float('inf')
                }
        
        # 减少并发数以避免资源竞争
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(test_single_source, row) for _, row in sources_df.iterrows()]
            
            completed = 0
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=25)  # 单个测速最大25秒超时
                    results.append(result)
                    completed += 1
                    
                    if result['accessible']:
                        print("✅", end="")
                    else:
                        print("❌", end="")
                        
                    # 每完成10个测速显示进度
                    if completed % 10 == 0:
                        print(f"({completed}/{total_sources})", end="")
                        
                except TimeoutError:
                    print("⏰", end="")
                    results.append({
                        'program_name': '超时频道',
                        'stream_url': '',
                        'accessible': False,
                        'speed': float('inf')
                    })
                except Exception as e:
                    print("💥", end="")
                    logger.debug(f"测速任务异常: {e}")
        
        print()  # 换行
        
        try:
            result_df = pd.DataFrame(results)
            accessible_df = result_df[result_df['accessible']].copy()
            accessible_df = accessible_df.sort_values(['program_name', 'speed'])
            
            accessible_count = len(accessible_df)
            avg_speed = accessible_df['speed'].mean() if not accessible_df.empty else 0
            
            logger.info(f"📊 测速完成: {accessible_count}/{total_sources} 个源可用")
            logger.info(f"📈 平均响应时间: {avg_speed:.2f} 秒")
            
            return accessible_df
            
        except Exception as e:
            logger.error(f"❌ 处理测速结果时出错: {e}")
            return pd.DataFrame()

    def generate_final_data(self, speed_tested_df: pd.DataFrame, template_categories: Dict[str, List[str]]) -> Dict[str, Any]:
        """生成最终数据"""
        logger.info("🎨 生成最终文件...")
        
        final_data = {}
        total_sources = 0
        
        if speed_tested_df.empty or not template_categories:
            logger.error("❌ 测速数据或模板分类为空")
            return final_data
        
        print("📦 生成进度: ", end="")
        
        for category, channels in template_categories.items():
            final_data[category] = {}
            
            for channel in channels:
                channel_sources = speed_tested_df[speed_tested_df['program_name'] == channel]
                
                if not channel_sources.empty:
                    sorted_sources = channel_sources.head(self.max_sources_per_channel)
                    final_data[category][channel] = sorted_sources[['stream_url', 'speed']].to_dict('records')
                    total_sources += len(sorted_sources)
                    print("✅", end="")
                else:
                    final_data[category][channel] = []
                    print("❌", end="")
        
        print()  # 换行
        logger.info(f"📦 总共收集到 {total_sources} 个有效源")
        return final_data

    def save_output_files(self, final_data: Dict[str, Any]) -> bool:
        """保存输出文件"""
        logger.info("💾 保存文件...")
        
        if not final_data:
            logger.error("❌ 没有数据需要保存")
            return False
        
        success_count = 0
        
        # 保存TXT格式
        try:
            with open(self.output_txt, 'w', encoding='utf-8') as f:
                for category, channels in final_data.items():
                    f.write(f"{category},#genre#\n")
                    
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f"{channel},{source['stream_url']}\n")
                    
                    f.write("\n")
            
            success_count += 1
            logger.info(f"✅ TXT文件已保存: {self.output_txt}")
            
        except Exception as e:
            logger.error(f"❌ 保存TXT文件失败: {e}")
        
        # 保存M3U格式
        try:
            with open(self.output_m3u, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                
                for category, channels in final_data.items():
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f'#EXTINF:-1 tvg-name="{channel}" group-title="{category}",{channel}\n')
                            f.write(f"{source['stream_url']}\n")
            
            success_count += 1
            logger.info(f"✅ M3U文件已保存: {self.output_m3u}")
            
        except Exception as e:
            logger.error(f"❌ 保存M3U文件失败: {e}")
            
        return success_count == 2

    def create_demo_template(self) -> bool:
        """创建示例模板文件"""
        demo_content = """央视频道,#genre#
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
"""
        try:
            with open(self.template_file, 'w', encoding='utf-8') as f:
                f.write(demo_content)
            logger.info(f"✅ 已创建示例模板文件: {self.template_file}")
            return True
        except Exception as e:
            logger.error(f"❌ 创建模板文件失败: {e}")
            return False

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
        
        for category, channels in final_data.items():
            category_channels = 0
            category_sources = 0
            
            for channel, sources in channels.items():
                if sources:
                    category_channels += 1
                    category_sources += len(sources)
            
            if category_channels > 0:
                print(f"  📺 {category}: {category_channels}频道, {category_sources}源")
                total_channels += category_channels
                total_sources += category_sources
        
        print("-"*50)
        print(f"📊 总计: {total_channels}频道, {total_sources}源")
        
        # 统计无源的频道
        no_source_channels = []
        for category, channels in final_data.items():
            for channel, sources in channels.items():
                if not sources:
                    no_source_channels.append(f"{category}-{channel}")
        
        if no_source_channels:
            print(f"⚠️  无源频道: {len(no_source_channels)}个")
            if len(no_source_channels) <= 5:
                for channel in no_source_channels:
                    print(f"    ❌ {channel}")

    def cleanup(self):
        """清理临时文件"""
        try:
            temp_dir = Path(self.temp_dir)
            if temp_dir.exists():
                for file in temp_dir.iterdir():
                    if file.is_file():
                        try:
                            file.unlink()
                        except:
                            pass
        except:
            pass

    def run(self):
        """主运行函数"""
        print("=" * 50)
        print("🎬 IPTV智能管理工具 - 核心功能版 v2.2")
        print("🔧 智能测速优化 (FFmpeg:10秒测试+10秒超时)")
        print("=" * 50)
        
        start_time = time.time()
        
        try:
            # 检查模板文件
            template_path = Path(self.template_file)
            if not template_path.exists():
                print("📝 未找到模板文件，创建示例模板...")
                if self.create_demo_template():
                    print(f"\n💡 模板文件已创建，请编辑后重新运行:")
                    print(f"   📄 {template_path.absolute()}")
                    input("按回车键退出...")
                return
            
            # 1. 加载模板
            print("\n📋 步骤 1/6: 加载频道模板")
            template_categories = self.load_template()
            if not template_categories:
                return
            
            # 2. 获取源数据
            print("\n🌐 步骤 2/6: 获取源数据")
            content = self.fetch_all_streams()
            if not content:
                print("❌ 未能获取任何源数据")
                return
            
            # 3. 整理源数据
            print("\n🔧 步骤 3/6: 整理源数据")
            sources_df = self.organize_streams(content)
            if sources_df.empty:
                print("❌ 未能解析出有效的流数据")
                return
            
            # 4. 获取所有模板频道
            all_template_channels = []
            for channels in template_categories.values():
                all_template_channels.extend(channels)
            
            # 5. 频道匹配
            print("\n🎯 步骤 4/6: 频道匹配")
            filtered_df = self.filter_and_sort_sources(sources_df, all_template_channels)
            if filtered_df.empty:
                print("❌ 没有匹配到任何模板频道")
                return
            
            # 6. 测速
            print("\n⚡ 步骤 5/6: 源测速")
            speed_tested_df = self.speed_test_sources(filtered_df)
            if speed_tested_df.empty:
                print("❌ 没有可用的源通过测速")
                return
            
            # 7. 生成最终数据
            print("\n🎨 步骤 6/6: 生成播放列表")
            final_data = self.generate_final_data(speed_tested_df, template_categories)
            
            # 8. 保存文件
            if not self.save_output_files(final_data):
                print("❌ 文件保存失败")
                return
            
            # 9. 打印统计
            self.print_statistics(final_data)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            print("\n🎉 处理完成!")
            print(f"⏰ 总耗时: {elapsed_time:.2f} 秒")
            print(f"📁 生成文件:")
            print(f"   📄 {Path(self.output_txt).absolute()}")
            print(f"   📄 {Path(self.output_m3u).absolute()}")
                
        except KeyboardInterrupt:
            print("\n⚠️  用户中断操作")
        except Exception as e:
            print(f"\n❌ 程序运行出错: {e}")
        finally:
            self.cleanup()

def main():
    """主函数"""
    try:
        manager = IPTVManager()
        manager.run()
    except Exception as e:
        print(f"❌ 程序启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
