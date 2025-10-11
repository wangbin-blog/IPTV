import requests
import pandas as pd
import re
import os
import time
import concurrent.futures
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urlparse

class IPTVTool:
    """IPTV直播源抓取与测速工具"""
    
    def __init__(self, timeout=8, max_workers=5, test_size_kb=64):
        """
        初始化工具
        
        Args:
            timeout: 请求超时时间（秒）
            max_workers: 最大并发线程数
            test_size_kb: 测速数据大小（KB）
        """
        # 配置参数
        self.timeout = timeout
        self.max_workers = max_workers
        self.test_size = test_size_kb * 1024
        
        # 请求会话配置
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Encoding': 'gzip, deflate'
        })
        
        # 数据源配置
        self.source_urls = [
            "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
            "https://live.zbds.top/tv/iptv6.txt", 
            "https://live.zbds.top/tv/iptv4.txt",
        ]
        
        # 正则表达式预编译
        self.ipv4_pattern = re.compile(r'^http://(\d{1,3}\.){3}\d{1,3}')
        self.ipv6_pattern = re.compile(r'^http://\[([a-fA-F0-9:]+)\]')
        self.channel_pattern = re.compile(r'^([^,#]+)')
        
        # 文件路径配置
        self.template_file = os.path.join(os.path.dirname(__file__), "demo.txt")
        self.output_files = {
            'txt': os.path.join(os.path.dirname(__file__), "iptv.txt"),
            'm3u': os.path.join(os.path.dirname(__file__), "iptv.m3u"),
            'log': os.path.join(os.path.dirname(__file__), "process.log")
        }
        
        # 初始化状态
        self.valid_channels = self.load_template_channels()
        self.setup_logging()

    def setup_logging(self):
        """初始化日志文件"""
        with open(self.output_files['log'], 'w', encoding='utf-8') as f:
            f.write(f"IPTV Tool Process Log - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*50 + "\n")

    def log(self, message: str, console_print=True):
        """记录日志"""
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}\n"
        
        with open(self.output_files['log'], 'a', encoding='utf-8') as f:
            f.write(log_entry)
        
        if console_print:
            print(message)

    def load_template_channels(self) -> Set[str]:
        """加载模板文件中的有效频道列表"""
        channels = set()
        if not os.path.exists(self.template_file):
            self.log(f"⚠️ 模板文件 {self.template_file} 不存在，将处理所有频道")
            return channels
        
        try:
            with open(self.template_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if match := self.channel_pattern.match(line):
                            channels.add(match.group(1).strip())
            self.log(f"加载模板频道 {len(channels)} 个")
        except Exception as e:
            self.log(f"加载模板文件错误: {str(e)}")
        
        return channels

    # ==================== 数据获取与处理 ====================
    
    def fetch_streams(self) -> Optional[str]:
        """从所有源URL抓取直播源"""
        contents = []
        for url in self.source_urls:
            self.log(f"抓取源: {url}")
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                contents.append(response.text)
            except Exception as e:
                self.log(f"抓取失败 {url}: {str(e)}")
        
        return "\n".join(contents) if contents else None

    def parse_content(self, content: str) -> pd.DataFrame:
        """解析直播源内容"""
        streams = []
        
        # 自动检测格式并解析
        if content.startswith("#EXTM3U"):
            current_program = None
            for line in content.splitlines():
                if line.startswith("#EXTINF"):
                    if match := re.search(r'tvg-name="([^"]+)"', line):
                        current_program = match.group(1).strip()
                elif line.startswith("http"):
                    if current_program:
                        streams.append({"program_name": current_program, "stream_url": line.strip()})
        else:
            for line in content.splitlines():
                if match := re.match(r"^([^,]+?)\s*,\s*(http.+)$", line):
                    streams.append({
                        "program_name": match.group(1).strip(),
                        "stream_url": match.group(2).strip()
                    })
        
        if not streams:
            return pd.DataFrame(columns=['program_name', 'stream_url'])
        
        df = pd.DataFrame(streams)
        
        # 过滤和去重
        if self.valid_channels:
            df = df[df['program_name'].isin(self.valid_channels)]
        
        return df.drop_duplicates(subset=['program_name', 'stream_url'])

    def organize_streams(self, df: pd.DataFrame) -> pd.DataFrame:
        """整理直播源数据"""
        return df.groupby('program_name')['stream_url'].apply(list).reset_index()

    # ==================== 测速功能 ====================
    
    def test_single_url(self, url: str) -> Tuple[Optional[float], Optional[str]]:
        """测试单个URL的速度"""
        try:
            start_time = time.time()
            with self.session.get(url, timeout=self.timeout, stream=True) as response:
                response.raise_for_status()
                
                content_length = 0
                for chunk in response.iter_content(chunk_size=8192):
                    content_length += len(chunk)
                    if content_length >= self.test_size:
                        break
                
                speed = content_length / (time.time() - start_time) / 1024
                return (speed, None) if speed > 0 else (0, "零速度")
                
        except requests.exceptions.Timeout:
            return (None, "超时")
        except requests.exceptions.SSLError:
            return (None, "SSL错误")
        except requests.exceptions.ConnectionError:
            return (None, "连接失败")
        except requests.exceptions.HTTPError as e:
            return (None, f"HTTP错误 {e.response.status_code}")
        except Exception as e:
            return (None, f"错误: {str(e)}")

    def test_urls_concurrently(self, urls: List[str]) -> List[Tuple[str, Optional[float], Optional[str]]]:
        """并发测试URL列表"""
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.test_single_url, url): url for url in urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                speed, error = future.result()
                results.append((url, speed, error))
        return results

    def test_all_channels(self, grouped_streams: pd.DataFrame, max_test=8, keep_best=8) -> Dict[str, List[Tuple[str, float]]]:
        """测试所有频道并保留最佳源"""
        results = {}
        total_channels = len(grouped_streams)
        
        self.log(f"开始测速 {total_channels} 个频道")
        
        for idx, (_, row) in enumerate(grouped_streams.iterrows(), 1):
            channel = row['program_name']
            urls = row['stream_url'][:max_test]
            
            self.log(f"[{idx}/{total_channels}] 测试频道: {channel} ({len(urls)}个源)")
            
            test_results = self.test_urls_concurrently(urls)
            valid_streams = []
            
            for url, speed, error in test_results:
                if speed is not None:
                    valid_streams.append((url, speed))
                    status = "✓" if speed > 100 else "⚠️"
                    self.log(f"    {status} {self._extract_domain(url)}: {speed:.1f} KB/s")
                else:
                    self.log(f"    ✗ {self._extract_domain(url)}: {error}")
            
            valid_streams.sort(key=lambda x: x[1], reverse=True)
            results[channel] = valid_streams[:keep_best]
            
            if results[channel]:
                best_speed = results[channel][0][1]
                self.log(f"    ✅ 最佳源: {best_speed:.1f} KB/s (保留{len(results[channel])}个)")
            else:
                self.log("    ❌ 无有效源")
        
        return results

    # ==================== 结果输出 ====================
    
    def generate_output_files(self, speed_results: Dict[str, List[Tuple[str, float]]]):
        """生成所有输出文件"""
        self.generate_txt_file(speed_results)
        self.generate_m3u_file(speed_results)
        self.generate_report(speed_results)

    def generate_txt_file(self, results: Dict[str, List[Tuple[str, float]]]):
        """生成TXT格式文件"""
        categories = {
            "央视频道,#genre#": ["CCTV", "央视"],
            "卫视频道,#genre#": ["卫视", "湖南", "浙江", "江苏", "东方", "北京"],
            "地方频道,#genre#": ["重庆", "广东", "深圳", "南方"],
            "其他频道,#genre#": []
        }
        
        categorized = {cat: [] for cat in categories}
        
        for channel in self.get_ordered_channels(results.keys()):
            streams = results.get(channel, [])
            if not streams:
                continue
                
            matched = False
            for cat, keywords in categories.items():
                if any(keyword in channel for keyword in keywords):
                    categorized[cat].extend(
                        f"{channel},{url} # 速度: {speed:.1f}KB/s" 
                        for url, speed in streams
                    )
                    matched = True
                    break
            
            if not matched:
                categorized["其他频道,#genre#"].extend(
                    f"{channel},{url} # 速度: {speed:.1f}KB/s" 
                    for url, speed in streams
                )
        
        with open(self.output_files['txt'], 'w', encoding='utf-8') as f:
            for cat, items in categorized.items():
                if items:
                    f.write(f"\n{cat}\n")
                    f.write("\n".join(items) + "\n")
        
        self.log(f"生成TXT文件: {self.output_files['txt']}")

    def generate_m3u_file(self, results: Dict[str, List[Tuple[str, float]]]):
        """生成M3U格式文件"""
        with open(self.output_files['m3u'], 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            
            # 写入分类信息
            f.write('#EXTINF:-1 group-title="央视频道",央视频道\n')
            f.write('#EXTINF:-1 group-title="卫视频道",卫视频道\n')
            f.write('#EXTINF:-1 group-title="地方频道",地方频道\n')
            f.write('#EXTINF:-1 group-title="其他频道",其他频道\n')
            
            for channel in self.get_ordered_channels(results.keys()):
                streams = results.get(channel, [])
                for url, speed in streams:
                    quality = self.get_speed_quality(speed)
                    f.write(f'#EXTINF:-1 tvg-name="{channel}",{channel} [速度: {speed:.1f}KB/s {quality}]\n{url}\n')
        
        self.log(f"生成M3U文件: {self.output_files['m3u']}")

    def generate_report(self, results: Dict[str, List[Tuple[str, float]]]):
        """生成测速报告"""
        speed_stats = []
        valid_channels = []
        
        for channel, streams in results.items():
            if streams:
                best_speed = streams[0][1]
                speed_stats.append(best_speed)
                valid_channels.append((channel, best_speed, len(streams)))
        
        if not speed_stats:
            self.log("⚠️ 无有效测速结果")
            return
        
        # 按速度排序频道
        valid_channels.sort(key=lambda x: x[1], reverse=True)
        
        report = [
            "="*50,
            "测速报告",
            "="*50,
            f"有效频道数: {len(valid_channels)}",
            f"总源数量: {sum(x[2] for x in valid_channels)}",
            f"平均速度: {sum(speed_stats)/len(speed_stats):.1f} KB/s",
            f"最快速度: {max(speed_stats):.1f} KB/s",
            f"最慢速度: {min(speed_stats):.1f} KB/s",
            "\n频道速度排名:"
        ]
        
        for i, (channel, speed, count) in enumerate(valid_channels[:20], 1):
            report.append(f"{i:2d}. {channel}: {speed:.1f} KB/s ({count}个源)")
        
        if len(valid_channels) > 20:
            report.append(f"...(共{len(valid_channels)}个频道)")
        
        report_content = "\n".join(report)
        self.log("\n" + report_content)
        
        with open(self.output_files['log'], 'a', encoding='utf-8') as f:
            f.write("\n" + report_content + "\n")

    # ==================== 辅助方法 ====================
    
    def get_ordered_channels(self, channels: List[str]) -> List[str]:
        """按照模板顺序排序频道列表"""
        if not self.valid_channels:
            return sorted(channels)
        
        ordered = []
        with open(self.template_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if match := self.channel_pattern.match(line):
                        channel = match.group(1).strip()
                        if channel in channels and channel not in ordered:
                            ordered.append(channel)
        
        # 添加未在模板中的频道
        for channel in channels:
            if channel not in ordered:
                ordered.append(channel)
                
        return ordered

    def _extract_domain(self, url: str) -> str:
        """从URL提取域名"""
        try:
            netloc = urlparse(url).netloc
            return netloc.split(':')[0]  # 移除端口号
        except:
            return url[:30] + "..." if len(url) > 30 else url

    def get_speed_quality(self, speed: float) -> str:
        """获取速度质量评级"""
        if speed > 1000: return "极佳"
        if speed > 500: return "优秀"
        if speed > 200: return "良好"
        if speed > 100: return "一般"
        if speed > 50: return "较差"
        return "极差"

    # ==================== 主流程 ====================
    
    def run(self):
        """运行主流程"""
        self.log("="*50)
        self.log("IPTV直播源处理工具")
        self.log("="*50)
        
        # 显示模板信息
        if self.valid_channels:
            self.log(f"模板频道: {len(self.valid_channels)}个")
        else:
            self.log("⚠️ 未使用模板过滤")
        
        # 抓取和处理数据
        self.log("\n开始抓取直播源...")
        if content := self.fetch_streams():
            self.log("\n解析直播源数据...")
            df = self.parse_content(content)
            
            # 显示频道匹配情况
            matched = set(df['program_name'].unique())
            self.log(f"\n频道匹配结果:")
            self.log(f"  发现频道总数: {len(matched)}")
            
            if self.valid_channels:
                unmatched = self.valid_channels - matched
                self.log(f"  匹配模板频道: {len(matched & self.valid_channels)}/{len(self.valid_channels)}")
                if unmatched:
                    self.log(f"  未匹配模板频道: {len(unmatched)}个")
            
            # 整理和组织数据
            grouped = self.organize_streams(df)
            self.log(f"\n有效直播源: {len(grouped)}个频道")
            
            # 测速和优化
            self.log("\n开始测速(每个频道测试最多8个源，保留最佳8个)...")
            speed_results = self.test_all_channels(grouped)
            
            # 生成输出文件
            self.log("\n生成输出文件中...")
            self.generate_output_files(speed_results)
            
            self.log("\n🎉 处理完成！")
        else:
            self.log("⚠️ 未能获取有效数据")

if __name__ == "__main__":
    # 配置参数
    config = {
        'timeout': 6,      # 请求超时时间(秒)
        'max_workers': 3,  # 最大并发数
        'test_size_kb': 32 # 测速数据大小(KB)
    }
    
    tool = IPTVTool(**config)
    tool.run()
