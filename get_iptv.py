#!/usr/bin/env python3
import requests
import pandas as pd
import re
import os
import time
import subprocess
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
import platform
import shutil

class IPTVManager:
    def __init__(self):
        # 配置文件
        self.SOURCE_URLS = [
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
        
        self.REQUEST_CONFIG = {
            'timeout': 20,
            'retries': 3,
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        }
        
        self.CHANNEL_CONFIG = {
            'max_sources_per_channel': 8,
            'speed_test_timeout': 8,
            'min_similarity_score': 60,
            'max_workers': min(8, os.cpu_count() or 4),  # 智能设置工作线程数
        }
        
        self.FILE_CONFIG = {
            'template_file': 'demo.txt',
            'output_txt': 'iptv.txt',
            'output_m3u': 'iptv.m3u',
            'temp_dir': 'temp',
            'log_file': 'iptv.log'
        }
        
        # 初始化系统
        self._setup_logging()
        self._init_session()
        self._create_directories()
        self._compile_regex()
        
        # 状态变量
        self.ffmpeg_available = False
        self.processed_count = 0
        self.total_count = 0
        self.start_time = 0
        self.is_terminal = sys.stdout.isatty()  # 检测是否在终端中运行

    def _setup_logging(self):
        """配置日志系统"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.FILE_CONFIG['log_file'], encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _init_session(self):
        """初始化请求会话"""
        self.session = requests.Session()
        self.session.headers.update(self.REQUEST_CONFIG['headers'])
        # 添加重试策略
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _create_directories(self):
        """创建必要的目录"""
        os.makedirs(self.FILE_CONFIG['temp_dir'], exist_ok=True)

    def _compile_regex(self):
        """编译正则表达式"""
        self.ipv4_pattern = re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}')
        self.ipv6_pattern = re.compile(r'^https?://\[([a-fA-F0-9:]+)\]')
        self.extinf_pattern = re.compile(r'#EXTINF:.*?tvg-name="([^"]+)".*?,(.+)')
        self.category_pattern = re.compile(r'^(.*?),#genre#$')
        self.url_pattern = re.compile(r'https?://[^\s,]+')
        self.channel_pattern = re.compile(r'^([^,]+),?')

    def _print_progress(self, current: int, total: int, prefix: str = '', suffix: str = '', bar_length: int = 50):
        """显示进度条（仅在终端中显示）"""
        if not self.is_terminal or total == 0:
            return
            
        percent = min(1.0, float(current) / total)
        arrow_length = int(round(percent * bar_length))
        arrow = '=' * arrow_length
        if arrow_length < bar_length:
            arrow += '>'
        spaces = ' ' * (bar_length - len(arrow))
        
        # 计算预计剩余时间
        eta_str = ""
        if current > 0 and hasattr(self, 'start_time') and self.start_time > 0:
            elapsed = time.time() - self.start_time
            if elapsed > 0:
                eta = (elapsed / current) * (total - current)
                if eta < 60:
                    eta_str = f"ETA: {eta:.0f}s"
                else:
                    eta_str = f"ETA: {eta/60:.1f}m"
        
        progress_text = f"\r{prefix}[{arrow}{spaces}] {int(round(percent * 100))}% {current}/{total} {eta_str} {suffix}"
        sys.stdout.write(progress_text.ljust(100))
        sys.stdout.flush()

    def check_dependencies(self) -> bool:
        """检查必要的依赖"""
        try:
            import requests
            import pandas as pd
            
            # 检查pandas版本
            pd_version = pd.__version__
            self.logger.info(f"✅ Pandas版本: {pd_version}")
            
        except ImportError as e:
            self.logger.error(f"❌ 缺少依赖: {e}")
            self.logger.error("💡 请运行: pip install requests pandas")
            return False
            
        # 检查FFmpeg
        self.ffmpeg_available = self._check_ffmpeg()
        return True

    def _check_ffmpeg(self) -> bool:
        """检查FFmpeg可用性"""
        # 首先检查环境变量中的ffmpeg
        ffmpeg_path = shutil.which('ffmpeg')
        if not ffmpeg_path:
            self.logger.warning("⚠️ FFmpeg未在PATH中找到")
            return False
            
        try:
            result = subprocess.run(
                [ffmpeg_path, '-version'], 
                capture_output=True, 
                timeout=5, 
                text=True,
                check=False
            )
            if result.returncode == 0:
                version_line = result.stdout.split('\n')[0] if result.stdout else "未知版本"
                self.logger.info(f"✅ FFmpeg可用: {version_line}")
                return True
            else:
                self.logger.warning("⚠️ FFmpeg检查失败")
                return False
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            self.logger.warning(f"⚠️ FFmpeg检查异常: {e}")
            return False

    def validate_url(self, url: str) -> bool:
        """验证URL格式"""
        try:
            result = urlparse(url)
            if not all([result.scheme in ['http', 'https'], result.netloc]):
                return False
            
            # 检查常见的不合法URL模式
            invalid_patterns = [
                'example.com',
                'localhost',
                '127.0.0.1',
                '0.0.0.0',
            ]
            
            if any(pattern in url.lower() for pattern in invalid_patterns):
                return False
                
            return True
        except Exception:
            return False

    def fetch_streams_from_url(self, url: str, retry: int = 0) -> Optional[str]:
        """从URL获取流数据"""
        try:
            response = self.session.get(url, timeout=self.REQUEST_CONFIG['timeout'])
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                content_length = len(response.text)
                if content_length < 100:  # 内容太短可能是错误页面
                    self.logger.warning(f"⚠️ 内容过短 ({content_length}字符) - {url}")
                    return None
                    
                self.logger.debug(f"✅ 成功获取 {urlparse(url).netloc}: {content_length} 字符")
                return response.text
            else:
                self.logger.warning(f"⚠️ HTTP {response.status_code} - {url}")
                
        except requests.exceptions.Timeout:
            self.logger.warning(f"⏰ 请求超时 - {url}")
        except requests.exceptions.ConnectionError:
            self.logger.warning(f"🔌 连接错误 - {url}")
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"🌐 网络错误: {e} - {url}")
        except Exception as e:
            self.logger.warning(f"❌ 请求异常: {e} - {url}")
            
        # 重试逻辑
        if retry < self.REQUEST_CONFIG['retries']:
            wait_time = 2 ** retry
            self.logger.info(f"🔄 重试({retry+1}/{self.REQUEST_CONFIG['retries']}) {wait_time}s后: {url}")
            time.sleep(wait_time)
            return self.fetch_streams_from_url(url, retry + 1)
            
        return None

    def fetch_all_streams(self) -> str:
        """获取所有源的流数据"""
        self.logger.info("🚀 开始智能多源抓取...")
        self.logger.info(f"📡 源数量: {len(self.SOURCE_URLS)}")
        
        all_streams = []
        successful_sources = 0
        failed_sources = []
        self.start_time = time.time()
        
        def fetch_with_progress(url):
            nonlocal successful_sources
            content = self.fetch_streams_from_url(url)
            if content:
                all_streams.append(content)
                successful_sources += 1
                return True, url
            else:
                failed_sources.append(urlparse(url).netloc)
                return False, url
        
        # 使用线程池并发抓取
        max_workers = min(len(self.SOURCE_URLS), self.CHANNEL_CONFIG['max_workers'])
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(fetch_with_progress, url): url for url in self.SOURCE_URLS}
            
            for i, future in enumerate(as_completed(future_to_url), 1):
                url = future_to_url[future]
                try:
                    success, source_url = future.result(timeout=30)
                    status = "✅" if success else "❌"
                    
                    # 更新进度
                    self._print_progress(
                        i, len(self.SOURCE_URLS),
                        prefix="🌐 抓取进度:",
                        suffix=f"成功: {successful_sources}/{i} | 当前: {urlparse(url).netloc} {status}"
                    )
                except Exception as e:
                    self.logger.error(f"❌ 处理 {url} 时发生错误: {e}")
                    failed_sources.append(urlparse(url).netloc)
        
        if self.is_terminal:
            print()  # 换行
            
        self.logger.info(f"📊 抓取完成: {successful_sources}/{len(self.SOURCE_URLS)} 个源成功")
        if failed_sources:
            self.logger.info(f"⚠️ 失败源: {', '.join(failed_sources[:5])}{'...' if len(failed_sources) > 5 else ''}")
        
        return "\n".join(all_streams)

    def parse_m3u(self, content: str) -> List[Dict]:
        """解析M3U格式"""
        streams = []
        current_program = None
        current_group = None
        
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
                
            if line.startswith("#EXTINF"):
                current_program = "未知频道"
                current_group = "默认分组"
                
                # 提取频道名称
                if match := re.search(r'tvg-name="([^"]+)"', line):
                    current_program = match.group(1).strip()
                elif match := re.search(r'#EXTINF:.*?,(.+)', line):
                    current_program = match.group(1).strip()
                
                # 提取分组信息
                if match := re.search(r'group-title="([^"]+)"', line):
                    current_group = match.group(1).strip()
                    
            elif line.startswith(('http://', 'https://')):
                if current_program and self.validate_url(line):
                    streams.append({
                        "program_name": current_program,
                        "stream_url": line,
                        "group": current_group
                    })
                current_program = None
                current_group = None
        
        return streams

    def parse_txt(self, content: str) -> List[Dict]:
        """解析TXT格式"""
        streams = []
        current_group = "默认分组"
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            # 检测分类行
            if match := self.category_pattern.match(line):
                current_group = match.group(1).strip()
                continue
                
            # 处理频道行
            if match := self.channel_pattern.match(line):
                program_name = match.group(1).strip()
                url_match = self.url_pattern.search(line)
                if url_match:
                    stream_url = url_match.group()
                    if self.validate_url(stream_url):
                        streams.append({
                            "program_name": program_name,
                            "stream_url": stream_url,
                            "group": current_group
                        })
        
        return streams

    def organize_streams(self, content: str) -> pd.DataFrame:
        """整理流数据"""
        if not content:
            self.logger.error("❌ 没有内容可处理")
            return pd.DataFrame()
            
        self.logger.info("🔍 解析流数据...")
        
        # 自动检测格式并解析
        if content.startswith("#EXTM3U"):
            streams = self.parse_m3u(content)
        else:
            streams = self.parse_txt(content)
        
        if not streams:
            self.logger.error("❌ 未能解析出任何流数据")
            return pd.DataFrame()
            
        df = pd.DataFrame(streams)
        
        # 数据清理
        initial_count = len(df)
        if initial_count == 0:
            self.logger.error("❌ 没有有效的流数据")
            return pd.DataFrame()
            
        df = df.dropna()
        df = df[df['program_name'].str.len() > 0]
        df = df[df['stream_url'].str.startswith(('http://', 'https://'))]
        
        # 去重
        df = df.drop_duplicates(subset=['program_name', 'stream_url'])
        
        # 清理频道名称
        df['program_name'] = df['program_name'].str.strip()
        
        self.logger.info(f"🧹 数据清理: {initial_count} → {len(df)} 个流")
        return df

    def load_template(self) -> Optional[Dict]:
        """加载频道模板"""
        template_file = self.FILE_CONFIG['template_file']
        if not os.path.exists(template_file):
            self.logger.error(f"❌ 模板文件 {template_file} 不存在")
            return None
            
        self.logger.info("📋 加载模板文件...")
        categories = {}
        current_category = None
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    # 检测分类行
                    if match := self.category_pattern.match(line):
                        current_category = match.group(1).strip()
                        categories[current_category] = []
                    elif current_category and line and not line.startswith('#'):
                        # 频道行
                        if match := self.channel_pattern.match(line):
                            channel_name = match.group(1).strip()
                            if channel_name:
                                categories[current_category].append(channel_name)
        except Exception as e:
            self.logger.error(f"❌ 读取模板文件失败: {e}")
            return None
        
        if not categories:
            self.logger.error("❌ 模板文件中未找到有效的频道分类")
            return None
            
        self.logger.info(f"📁 模板分类: {list(categories.keys())}")
        total_channels = sum(len(channels) for channels in categories.values())
        self.logger.info(f"📺 模板频道总数: {total_channels}")
        
        return categories

    def similarity_score(self, str1: str, str2: str) -> int:
        """计算两个字符串的相似度分数"""
        if not str1 or not str2:
            return 0
            
        # 预处理字符串
        str1_clean = re.sub(r'[^\w]', '', str1.lower())
        str2_clean = re.sub(r'[^\w]', '', str2.lower())
        
        # 完全匹配
        if str1_clean == str2_clean:
            return 100
        
        # 包含关系（双向）
        if str1_clean in str2_clean:
            return 90
        if str2_clean in str1_clean:
            return 85
        
        # 使用difflib计算相似度
        try:
            similarity = SequenceMatcher(None, str1_clean, str2_clean).ratio()
            score = int(similarity * 80)
            
            # 关键词匹配加分
            keywords = ['cctv', '卫视', 'tv', 'hd', 'fhd', '4k']
            for keyword in keywords:
                if keyword in str1_clean and keyword in str2_clean:
                    score += 5
                    
            return min(score, 100)
        except Exception:
            # 备用方案：简单的共同字符比例
            common_chars = len(set(str1_clean) & set(str2_clean))
            total_chars = len(set(str1_clean) | set(str2_clean))
            
            if total_chars > 0:
                similarity = (common_chars / total_chars) * 80
                return int(similarity)
        
        return 0

    def speed_test_ffmpeg(self, stream_url: str) -> Tuple[bool, float]:
        """使用FFmpeg进行流媒体测速"""
        if not self.ffmpeg_available:
            return False, float('inf')
            
        temp_file = os.path.join(self.FILE_CONFIG['temp_dir'], f'test_{abs(hash(stream_url))}.ts')
        
        try:
            # 构建FFmpeg命令（兼容不同版本）
            cmd = [
                'ffmpeg',
                '-y',
                '-loglevel', 'quiet',
                '-i', stream_url,
                '-t', '3',
                '-c', 'copy',
                '-f', 'mpegts',
                temp_file
            ]
            
            start_time = time.time()
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=self.CHANNEL_CONFIG['speed_test_timeout'],
                check=False
            )
            end_time = time.time()
            
            # 清理临时文件
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            
            if result.returncode == 0:
                speed = end_time - start_time
                return True, speed
            else:
                return False, float('inf')
                
        except (subprocess.TimeoutExpired, Exception):
            # 清理临时文件
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            return False, float('inf')

    def speed_test_simple(self, stream_url: str) -> Tuple[bool, float]:
        """简单的HTTP测速"""
        try:
            start_time = time.time()
            response = self.session.head(
                stream_url, 
                timeout=self.CHANNEL_CONFIG['speed_test_timeout'],
                allow_redirects=True
            )
            end_time = time.time()
            
            if response.status_code in [200, 302, 301, 307]:
                return True, end_time - start_time
            else:
                return False, float('inf')
        except Exception:
            return False, float('inf')

    def speed_test_sources(self, sources_df: pd.DataFrame) -> pd.DataFrame:
        """对源进行测速"""
        self.logger.info("⚡ 开始智能测速...")
        self.logger.info(f"📊 待测速源总数: {len(sources_df)}")
        
        if sources_df.empty:
            self.logger.error("❌ 没有需要测速的源")
            return pd.DataFrame()
            
        results = []
        total_sources = len(sources_df)
        self.total_count = total_sources
        self.processed_count = 0
        self.start_time = time.time()
        
        # 进度计数器
        tested_count = 0
        success_count = 0
        
        def test_single_source(row):
            nonlocal tested_count, success_count
            program_name = row['program_name']
            stream_url = row['stream_url']
            
            self.processed_count += 1
            current = self.processed_count
            
            # 更新进度条
            self._print_progress(
                current, total_sources,
                prefix="📶 测速进度:",
                suffix=f"成功: {success_count}/{tested_count} | 当前: {program_name[:12]}..."
            )
            
            # 智能选择测速方式
            if any(ext in stream_url.lower() for ext in ['.m3u8', '.ts', '.flv', '.mp4', '.mpeg', '.avi']):
                if self.ffmpeg_available:
                    accessible, speed = self.speed_test_ffmpeg(stream_url)
                else:
                    accessible, speed = self.speed_test_simple(stream_url)
            else:
                accessible, speed = self.speed_test_simple(stream_url)
            
            tested_count += 1
            if accessible:
                success_count += 1
            
            return {
                'program_name': program_name,
                'stream_url': stream_url,
                'accessible': accessible,
                'speed': speed
            }
        
        # 使用线程池进行并发测速
        max_workers = min(total_sources, self.CHANNEL_CONFIG['max_workers'])
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(test_single_source, row) for _, row in sources_df.iterrows()]
            
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=15)
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"❌ 测速异常: {e}")
        
        # 完成进度条
        if self.is_terminal:
            self._print_progress(total_sources, total_sources, prefix="✅ 测速完成:", suffix="\n")
        
        # 过滤不可访问的源
        accessible_df = pd.DataFrame(results)
        if accessible_df.empty:
            self.logger.error("❌ 没有可用的源通过测速")
            return accessible_df
            
        accessible_df = accessible_df[accessible_df['accessible']].copy()
        
        success_rate = len(accessible_df) / total_sources if total_sources > 0 else 0
        self.logger.info(f"📊 测速结果: {len(accessible_df)}/{total_sources} 个源可用 (成功率: {success_rate:.1%})")
        
        return accessible_df

    def filter_and_sort_sources(self, sources_df: pd.DataFrame, template_categories: Dict) -> pd.DataFrame:
        """根据模板过滤和排序源"""
        self.logger.info("🎯 开始频道匹配...")
        
        # 获取所有模板频道（保持顺序）
        all_template_channels = []
        for category_channels in template_categories.values():
            all_template_channels.extend(category_channels)
        
        self.logger.info(f"📋 模板频道数: {len(all_template_channels)}")
        self.logger.info(f"📡 可用源数量: {len(sources_df)}")
        
        channel_mapping = {}
        match_results = []
        
        # 为每个模板频道寻找最佳匹配
        for template_channel in all_template_channels:
            best_match = None
            best_score = 0
            best_source_channel = None
            
            # 在源数据中寻找最佳匹配
            for source_channel in sources_df['program_name'].unique():
                score = self.similarity_score(template_channel, source_channel)
                if score > best_score and score >= self.CHANNEL_CONFIG['min_similarity_score']:
                    best_score = score
                    best_match = template_channel
                    best_source_channel = source_channel
            
            if best_match and best_source_channel:
                channel_mapping[best_source_channel] = best_match
                match_results.append((best_match, best_source_channel, best_score))
        
        # 打印匹配结果
        if match_results:
            self.logger.info("\n🏆 最佳匹配结果:")
            displayed_matches = 0
            for template_channel, source_channel, score in sorted(match_results, key=lambda x: x[2], reverse=True):
                if displayed_matches < 15:
                    status = "✅" if score >= 80 else "⚠️"
                    self.logger.info(f"  {status} {template_channel[:18]:<18} ← {source_channel[:18]:<18} (匹配度: {score}%)")
                    displayed_matches += 1
            
            if len(match_results) > 15:
                self.logger.info(f"  ... 还有 {len(match_results) - 15} 个匹配")
        else:
            self.logger.warning("⚠️ 没有找到匹配的频道")
        
        # 过滤数据，只保留匹配的频道
        if not channel_mapping:
            self.logger.error("❌ 没有找到任何匹配的频道")
            return pd.DataFrame()
            
        matched_mask = sources_df['program_name'].isin(channel_mapping.keys())
        filtered_df = sources_df[matched_mask].copy()
        
        if filtered_df.empty:
            self.logger.error("❌ 过滤后没有数据")
            return filtered_df
            
        # 将源频道名称映射回模板频道名称
        filtered_df['program_name'] = filtered_df['program_name'].map(channel_mapping)
        
        self.logger.info(f"🎉 频道匹配完成: {len(filtered_df)} 个流匹配到 {len(set(channel_mapping.values()))} 个模板频道")
        return filtered_df

    def generate_final_data(self, speed_tested_df: pd.DataFrame, template_categories: Dict) -> Dict:
        """生成最终数据"""
        self.logger.info("📺 生成播放列表...")
        
        final_data = {}
        total_sources = 0
        
        # 严格按照模板分类和频道顺序
        for category, channels in template_categories.items():
            final_data[category] = {}
            
            for channel in channels:
                # 获取该频道的所有源
                channel_sources = speed_tested_df[speed_tested_df['program_name'] == channel]
                
                if not channel_sources.empty:
                    # 按速度排序并取前8个
                    sorted_sources = channel_sources.sort_values('speed').head(
                        self.CHANNEL_CONFIG['max_sources_per_channel']
                    )
                    final_data[category][channel] = sorted_sources[['stream_url', 'speed']].to_dict('records')
                    source_count = len(sorted_sources)
                    total_sources += source_count
                    
                    # 显示源质量信息
                    if source_count > 0:
                        best_speed = sorted_sources.iloc[0]['speed']
                        speed_str = f"{best_speed:.2f}s" if best_speed < 10 else ">10s"
                        self.logger.info(f"  ✅ {category[:8]:<8}-{channel[:16]:<16}: {source_count}源 (最佳: {speed_str})")
                else:
                    final_data[category][channel] = []
                    self.logger.warning(f"  ❌ {category[:8]:<8}-{channel[:16]:<16}: 无可用源")
        
        self.logger.info(f"📊 总共收集到 {total_sources} 个有效源")
        return final_data

    def save_output_files(self, final_data: Dict) -> bool:
        """保存输出文件"""
        self.logger.info("💾 保存文件...")
        
        success = True
        
        # 保存TXT格式
        try:
            output_txt = self.FILE_CONFIG['output_txt']
            with open(output_txt, 'w', encoding='utf-8') as f:
                for category, channels in final_data.items():
                    f.write(f"{category},#genre#\n")
                    
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f"{channel},{source['stream_url']}\n")
                    
                    f.write("\n")
            self.logger.info(f"✅ TXT文件已保存: {os.path.abspath(output_txt)}")
        except Exception as e:
            self.logger.error(f"❌ 保存TXT文件失败: {e}")
            success = False
        
        # 保存M3U格式
        try:
            output_m3u = self.FILE_CONFIG['output_m3u']
            with open(output_m3u, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                
                for category, channels in final_data.items():
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f'#EXTINF:-1 tvg-name="{channel}" group-title="{category}",{channel}\n')
                            f.write(f"{source['stream_url']}\n")
            self.logger.info(f"✅ M3U文件已保存: {os.path.abspath(output_m3u)}")
        except Exception as e:
            self.logger.error(f"❌ 保存M3U文件失败: {e}")
            success = False
            
        return success

    def print_statistics(self, final_data: Dict):
        """打印统计信息"""
        print("\n" + "="*60)
        print("📈 生成统计报告")
        print("="*60)
        
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
                avg_sources = category_sources / category_channels if category_channels > 0 else 0
                print(f"  📺 {category:<12}: {category_channels:2d}频道, {category_sources:3d}源 (平均: {avg_sources:.1f}源/频道)")
                total_channels += category_channels
                total_sources += category_sources
        
        print("-"*60)
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

    def cleanup(self):
        """清理临时文件"""
        try:
            temp_dir = self.FILE_CONFIG['temp_dir']
            if os.path.exists(temp_dir):
                for file in os.listdir(temp_dir):
                    file_path = os.path.join(temp_dir, file)
                    if os.path.isfile(file_path):
                        try:
                            os.remove(file_path)
                        except:
                            pass
                self.logger.debug("🧹 临时文件清理完成")
        except Exception as e:
            self.logger.error(f"❌ 清理临时文件时出错: {e}")

    def create_demo_template(self) -> bool:
        """创建示例模板文件"""
        demo_content = """央视频道,#genre#
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
深圳都市
重庆卫视
四川卫视
河南卫视
湖北卫视

高清频道,#genre#
CCTV-1 HD
CCTV-5 HD
湖南卫视 HD
浙江卫视 HD
江苏卫视 HD
东方卫视 HD
"""
        try:
            with open(self.FILE_CONFIG['template_file'], 'w', encoding='utf-8') as f:
                f.write(demo_content)
            self.logger.info(f"✅ 已创建示例模板文件: {self.FILE_CONFIG['template_file']}")
            self.logger.info("📝 请编辑此文件，添加您需要的频道列表")
            return True
        except Exception as e:
            self.logger.error(f"❌ 创建模板文件失败: {e}")
            return False

    def run(self):
        """主运行函数"""
        print("=" * 70)
        print("🎬 IPTV智能管理工具 - 优化版 v2.1")
        print("=" * 70)
        print("✨ 优化特性: 修复导入+终端检测+智能线程+兼容性提升")
        print("-" * 70)
        
        # 检查依赖
        if not self.check_dependencies():
            self.logger.error("❌ 依赖检查失败，程序退出")
            return
        
        # 检查模板文件，如果不存在则创建示例
        if not os.path.exists(self.FILE_CONFIG['template_file']):
            self.logger.info("📄 未找到模板文件，创建示例模板...")
            if not self.create_demo_template():
                return
            self.logger.info("💡 请编辑 demo.txt 文件，添加您需要的频道，然后重新运行程序")
            return
        
        start_time = time.time()
        
        try:
            # 1. 加载模板
            self.logger.info("\n📍 步骤 1/7: 加载频道模板")
            template_categories = self.load_template()
            if not template_categories:
                return
            
            # 2. 获取所有源数据
            self.logger.info("\n📍 步骤 2/7: 获取源数据")
            content = self.fetch_all_streams()
            if not content:
                self.logger.error("❌ 未能获取任何源数据")
                return
            
            # 3. 整理源数据
            self.logger.info("\n📍 步骤 3/7: 整理源数据")
            sources_df = self.organize_streams(content)
            if sources_df.empty:
                self.logger.error("❌ 未能解析出有效的流数据")
                return
            
            # 4. 过滤和匹配频道
            self.logger.info("\n📍 步骤 4/7: 频道匹配")
            filtered_df = self.filter_and_sort_sources(sources_df, template_categories)
            if filtered_df.empty:
                self.logger.error("❌ 没有匹配到任何模板频道")
                return
            
            # 5. 测速
            self.logger.info("\n📍 步骤 5/7: 源测速")
            speed_tested_df = self.speed_test_sources(filtered_df)
            if speed_tested_df.empty:
                self.logger.error("❌ 没有可用的源通过测速")
                return
            
            # 6. 生成最终数据
            self.logger.info("\n📍 步骤 6/7: 生成播放列表")
            final_data = self.generate_final_data(speed_tested_df, template_categories)
            
            # 7. 保存文件
            self.logger.info("\n📍 步骤 7/7: 保存文件")
            if not self.save_output_files(final_data):
                self.logger.error("❌ 文件保存失败")
                return
            
            # 8. 打印统计
            self.print_statistics(final_data)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            self.logger.info("\n🎉 处理完成!")
            self.logger.info(f"⏱️  总耗时: {elapsed_time:.2f} 秒")
            self.logger.info("📂 生成文件位置:")
            self.logger.info(f"  📄 {os.path.abspath(self.FILE_CONFIG['output_txt'])}")
            self.logger.info(f"  📄 {os.path.abspath(self.FILE_CONFIG['output_m3u'])}")
            
        except KeyboardInterrupt:
            self.logger.warning("⏹️  用户中断操作")
        except Exception as e:
            self.logger.error(f"❌ 程序运行出错: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            # 清理临时文件
            self.cleanup()

def main():
    """主函数"""
    try:
        manager = IPTVManager()
        manager.run()
    except KeyboardInterrupt:
        print("\n👋 用户退出程序")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 程序启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
