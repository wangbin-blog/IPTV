#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV直播源生成器 - 企业级完整版
功能整合：
1. 智能多源抓取与优先级管理
2. FFmpeg深度质量检测
3. 严格模板筛选机制
4. 频道接口数量限制
5. 集中式文件管理
"""

import os
import re
import sys
import time
import signal
import shlex
import socket
import logging
import tracemalloc
import subprocess
from urllib.parse import urlparse
from typing import Dict, List, Tuple, Optional, Set
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import yaml

# ==================== 配置管理系统 ====================
class Config:
    """集中式配置管理"""
    CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # 输入文件
    DEMO_TXT = os.path.join(CONFIG_DIR, "demo.txt")          # 频道模板
    SOURCE_URLS = os.path.join(CONFIG_DIR, "sources.txt")    # 源列表
    
    # 输出文件
    OUTPUT_IPV4_TXT = os.path.join(CONFIG_DIR, "iptv_ipv4.txt")
    OUTPUT_IPV6_TXT = os.path.join(CONFIG_DIR, "iptv_ipv6.txt")
    OUTPUT_IPV4_M3U = os.path.join(CONFIG_DIR, "iptv_ipv4.m3u")
    OUTPUT_IPV6_M3U = os.path.join(CONFIG_DIR, "iptv_ipv6.m3u")
    LOG_FILE = os.path.join(CONFIG_DIR, "iptv.log")
    
    # 运行参数
    MAX_SOURCES_PER_CHANNEL = 8      # 每个频道最大接口数
    FFMPEG_TIMEOUT = 5               # FFmpeg测速超时(秒)
    REQUEST_TIMEOUT = 15             # 网络请求超时(秒)
    MAX_RETRIES = 2                  # 最大重试次数
    MAX_WORKERS = 10                 # 最大线程数
    REQUEST_DELAY = 0.3              # 请求间隔(秒)
    
    # 智能测速参数
    SPEED_TEST_SAMPLES = 3           # 测速采样次数
    MIN_BITRATE = 500                # 最低有效比特率(kbps)

# ==================== 核心数据结构 ====================
@dataclass
class StreamSource:
    """流媒体源数据结构"""
    name: str
    url: str
    speed: float = float('inf')      # 连接速度(秒)
    bitrate: int = 0                 # 比特率(kbps)
    resolution: str = ""             # 分辨率
    source_priority: int = 0         # 源优先级
    is_ipv6: bool = False            # IPv6标识
    
    @property
    def quality_score(self) -> float:
        """综合质量评分"""
        speed_weight = 0.4 if self.is_ipv6 else 0.3  # IPv6连接速度权重更高
        return (self.bitrate / 2000) * 0.6 + (1 / (self.speed + 0.1)) * speed_weight

# ==================== 主生成器类 ====================
class IPTVGenerator:
    def __init__(self):
        """初始化生成器"""
        self._setup_logging()
        self._setup_signal_handlers()
        self._init_config_files()
        
        # 加载基础数据
        self.template_channels = self._load_template_channels()
        self.source_urls = self._load_source_urls()
        
        # 运行时状态
        self.stats = defaultdict(int)
        self.domain_stats = {}  # 域名性能统计
        self.stream_cache = OrderedDict()  # 流缓存

    # ================ 初始化方法 ================
    def _setup_logging(self):
        """配置日志系统"""
        self.logger = logging.getLogger('iptv_generator')
        self.logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 文件处理器
        file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def _setup_signal_handlers(self):
        """设置信号处理器"""
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)

    def _init_config_files(self):
        """初始化配置文件"""
        if not os.path.exists(Config.SOURCE_URLS):
            self._create_default_source_urls()
        if not os.path.exists(Config.DEMO_TXT):
            self._create_default_demo_template()

    def _create_default_source_urls(self):
        """创建默认源URL列表"""
        default_sources = [
            "https://raw.githubusercontent.com/iptv-org/iptv/master/streams.m3u",
            "https://mirror.ghproxy.com/https://raw.githubusercontent.com/freeiptv/iptv/master/playlist.m3u"
        ]
        with open(Config.SOURCE_URLS, 'w', encoding='utf-8') as f:
            f.write("\n".join(default_sources))
        self.logger.info("已创建默认源列表")

    def _create_default_demo_template(self):
        """创建默认频道模板"""
        default_template = """央视频道,#genre#
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

卫视频道,#genre#
湖南卫视
浙江卫视
江苏卫视
东方卫视
北京卫视
"""
        with open(Config.DEMO_TXT, 'w', encoding='utf-8') as f:
            f.write(default_template)
        self.logger.info("已创建默认频道模板")

    # ================ 数据加载方法 ================
    def _load_template_channels(self) -> Dict[str, List[str]]:
        """加载模板频道分类"""
        channels = {}
        current_category = "未分类"
        
        try:
            with open(Config.DEMO_TXT, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    if line.endswith(',#genre#'):
                        current_category = line.split(',')[0]
                        channels[current_category] = []
                    else:
                        channels.setdefault(current_category, []).append(line)
                        
            self.logger.info(f"加载模板频道: {len(channels)}个分类")
            return channels
            
        except Exception as e:
            self.logger.error(f"加载模板失败: {str(e)}")
            sys.exit(1)

    def _load_source_urls(self) -> List[str]:
        """加载源URL列表"""
        try:
            with open(Config.SOURCE_URLS, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
                
            # 验证URL有效性
            valid_urls = []
            for url in urls:
                if self._validate_url(url):
                    valid_urls.append(url)
                else:
                    self.logger.warning(f"忽略无效URL: {url}")
            
            if not valid_urls:
                raise ValueError("没有有效的源URL")
                
            self.logger.info(f"加载有效源URL: {len(valid_urls)}个")
            return valid_urls
            
        except Exception as e:
            self.logger.error(f"加载源列表失败: {str(e)}")
            sys.exit(1)

    def _validate_url(self, url: str) -> bool:
        """验证URL有效性"""
        try:
            result = urlparse(url)
            return all([
                result.scheme in ('http', 'https'),
                result.netloc,
                len(url) < 2048
            ])
        except ValueError:
            return False

    # ================ 核心业务逻辑 ================
    def run(self):
        """主运行流程"""
        self.logger.info("=== IPTV生成器启动 ===")
        start_time = time.time()
        
        try:
            # 阶段1: 多源抓取
            self.logger.info("开始多源抓取...")
            raw_data = self._fetch_all_sources()
            
            # 阶段2: 数据解析与过滤
            self.logger.info("解析直播源数据...")
            parsed_streams = self._parse_and_filter_streams(raw_data)
            
            # 阶段3: 智能测速
            self.logger.info("执行质量检测...")
            tested_streams = self._test_streams(parsed_streams)
            
            # 阶段4: 生成输出
            self.logger.info("生成输出文件...")
            self._generate_outputs(tested_streams)
            
            # 打印统计
            elapsed = time.time() - start_time
            self.logger.info(f"任务完成! 耗时: {elapsed:.2f}秒")
            self._print_stats()
            
        except Exception as e:
            self.logger.error(f"运行失败: {str(e)}", exc_info=True)
            sys.exit(1)

    def _fetch_all_sources(self) -> str:
        """多源并发抓取"""
        all_contents = []
        success_count = 0
        
        with ThreadPoolExecutor(max_workers=min(Config.MAX_WORKERS, len(self.source_urls))) as executor:
            futures = {executor.submit(self._fetch_source, url): url for url in self.source_urls}
            
            for future in as_completed(futures):
                url = futures[future]
                try:
                    if content := future.result():
                        all_contents.append(content)
                        success_count += 1
                        self.logger.info(f"成功获取: {url}")
                    else:
                        self.logger.warning(f"获取失败: {url}")
                except Exception as e:
                    self.logger.error(f"处理异常: {url} - {str(e)}")
                
                # 请求间隔控制
                time.sleep(Config.REQUEST_DELAY)
        
        if not all_contents:
            raise ValueError("所有源获取失败")
            
        self.stats['sources_fetched'] = success_count
        return "\n".join(all_contents)

    def _fetch_source(self, url: str) -> Optional[str]:
        """获取单个源数据"""
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                response = requests.get(
                    url,
                    timeout=Config.REQUEST_TIMEOUT,
                    headers={'User-Agent': 'IPTV Generator/2.0'}
                )
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                if attempt < Config.MAX_RETRIES:
                    time.sleep(1 * (attempt + 1))
                else:
                    self.logger.debug(f"请求失败 [{attempt+1}/{Config.MAX_RETRIES}]: {url}")
        return None

    def _parse_and_filter_streams(self, content: str) -> List[StreamSource]:
        """解析并过滤直播源"""
        streams = []
        
        # 解析M3U格式
        if content.startswith("#EXTM3U"):
            current_name = None
            for line in content.splitlines():
                line = line.strip()
                if line.startswith("#EXTINF"):
                    current_name = self._parse_extinf(line)
                elif line.startswith(("http://", "https://")):
                    if current_name and self._is_template_channel(current_name):
                        streams.append(StreamSource(
                            name=current_name,
                            url=line,
                            is_ipv6=self._is_ipv6_url(line)
                        ))
                        current_name = None
        # 解析TXT格式
        else:
            for line in content.splitlines():
                if "," in line:
                    name, url = line.split(",", 1)
                    name = name.strip()
                    if self._is_template_channel(name) and self._validate_url(url):
                        streams.append(StreamSource(
                            name=name,
                            url=url.strip(),
                            is_ipv6=self._is_ipv6_url(url)
                        ))
        
        if not streams:
            raise ValueError("未解析到有效直播源")
            
        self.stats['streams_parsed'] = len(streams)
        return streams

    def _is_template_channel(self, channel_name: str) -> bool:
        """检查是否为模板频道"""
        for channels in self.template_channels.values():
            if channel_name in channels:
                return True
        return False

    def _is_ipv6_url(self, url: str) -> bool:
        """检测是否为IPv6地址"""
        try:
            hostname = urlparse(url).hostname
            if not hostname:
                return False
                
            # 显式IPv6地址 (如 http://[2001:db8::1])
            if hostname.startswith('[') and hostname.endswith(']'):
                return True
                
            # DNS解析检测
            try:
                addr_info = socket.getaddrinfo(hostname, None)
                return any(info[0] == socket.AF_INET6 for info in addr_info)
            except socket.gaierror:
                return False
                
        except Exception:
            return False

    def _test_streams(self, streams: List[StreamSource]) -> Dict[str, List[StreamSource]]:
        """智能测速与筛选"""
        tested_streams = defaultdict(list)
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(self._test_stream, stream): stream for stream in streams}
            
            for future in as_completed(futures):
                stream = futures[future]
                try:
                    result = future.result()
                    if result['success'] and result['bitrate'] >= Config.MIN_BITRATE:
                        stream.speed = result['speed']
                        stream.bitrate = result['bitrate']
                        stream.resolution = result['resolution']
                        tested_streams[stream.name].append(stream)
                        self.stats['streams_passed'] += 1
                    self.stats['streams_tested'] += 1
                except Exception as e:
                    self.logger.debug(f"测速异常: {stream.url} - {str(e)}")
        
        # 按质量排序并限制数量
        final_streams = {}
        for name, stream_list in tested_streams.items():
            stream_list.sort(key=lambda x: (-x.quality_score, x.speed))
            final_streams[name] = stream_list[:Config.MAX_SOURCES_PER_CHANNEL]
        
        return final_streams

    def _test_stream(self, stream: StreamSource) -> Dict:
        """FFmpeg智能测速"""
        result = {
            'success': False,
            'speed': float('inf'),
            'bitrate': 0,
            'resolution': 'unknown'
        }
        
        try:
            start_time = time.time()
            cmd = [
                'ffmpeg',
                '-i', stream.url,
                '-f', 'null',
                '-',
                '-v', 'quiet',
                '-t', str(Config.FFMPEG_TIMEOUT)
            ]
            
            process = subprocess.run(
                cmd,
                check=True,
                timeout=Config.FFMPEG_TIMEOUT + 2,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # 解析输出
            output = process.stderr
            result.update({
                'success': True,
                'speed': time.time() - start_time,
                'bitrate': self._parse_bitrate(output),
                'resolution': self._parse_resolution(output)
            })
            
        except subprocess.TimeoutExpired:
            result['speed'] = float('inf')
        except Exception as e:
            self.logger.debug(f"测速失败 {stream.url}: {str(e)}")
            
        return result

    def _parse_bitrate(self, ffmpeg_output: str) -> int:
        """从FFmpeg输出解析比特率"""
        match = re.search(r'bitrate=\s*(\d+)\s*kb/s', ffmpeg_output)
        return int(match.group(1)) if match else 0

    def _parse_resolution(self, ffmpeg_output: str) -> str:
        """从FFmpeg输出解析分辨率"""
        match = re.search(r'Video:.*?(\d{3,4}x\d{3,4})', ffmpeg_output)
        return match.group(1) if match else 'unknown'

    def _generate_outputs(self, streams: Dict[str, List[StreamSource]]):
        """生成输出文件"""
        # 按模板分类排序
        sorted_streams = []
        for category, channel_list in self.template_channels.items():
            for channel in channel_list:
                if channel in streams:
                    sorted_streams.append((category, channel, streams[channel]))
        
        # 生成IPv4和IPv6分离的输出
        ipv4_data = defaultdict(list)
        ipv6_data = defaultdict(list)
        
        for category, channel, stream_list in sorted_streams:
            for stream in stream_list:
                if stream.is_ipv6:
                    ipv6_data[category].append((channel, stream))
                else:
                    ipv4_data[category].append((channel, stream))
        
        # 生成TXT文件
        self._generate_txt_file(Config.OUTPUT_IPV4_TXT, ipv4_data)
        self._generate_txt_file(Config.OUTPUT_IPV6_TXT, ipv6_data)
        
        # 生成M3U文件
        self._generate_m3u_file(Config.OUTPUT_IPV4_M3U, ipv4_data)
        self._generate_m3u_file(Config.OUTPUT_IPV6_M3U, ipv6_data)
        
        self.logger.info("输出文件生成完成")

    def _generate_txt_file(self, filepath: str, data: Dict[str, List[Tuple[str, StreamSource]]]):
        """生成TXT格式文件"""
        with open(filepath, 'w', encoding='utf-8') as f:
            for category, items in data.items():
                f.write(f"\n{category},#genre#\n")
                for channel, stream in items:
                    f.write(f"{channel},{stream.url}\n")

    def _generate_m3u_file(self, filepath: str, data: Dict[str, List[Tuple[str, StreamSource]]]):
        """生成M3U格式文件"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            for category, items in data.items():
                f.write(f'#EXTINF:-1 group-title="{category}",{category}\n')
                f.write("#genre#\n")
                for channel, stream in items:
                    f.write(f'#EXTINF:-1 group-title="{category}",{channel}\n')
                    f.write(f"{stream.url}\n")

    def _print_stats(self):
        """打印运行统计"""
        stats = [
            "=== 运行统计 ===",
            f"源URL获取: {self.stats['sources_fetched']}/{len(self.source_urls)}",
            f"解析流数: {self.stats['streams_parsed']}",
            f"测试流数: {self.stats['streams_tested']}",
            f"通过流数: {self.stats['streams_passed']}",
            f"频道数量: {len(self.template_channels)}",
            f"输出文件:",
            f"  {Config.OUTPUT_IPV4_TXT}",
            f"  {Config.OUTPUT_IPV6_TXT}",
            f"  {Config.OUTPUT_IPV4_M3U}",
            f"  {Config.OUTPUT_IPV6_M3U}"
        ]
        
        for line in stats:
            self.logger.info(line)

    def _handle_interrupt(self, signum, frame):
        """处理中断信号"""
        self.logger.warning(f"接收到终止信号 {signum}, 正在安全退出...")
        sys.exit(0)

# ==================== 主程序入口 ====================
if __name__ == "__main__":
    try:
        generator = IPTVGenerator()
        generator.run()
    except KeyboardInterrupt:
        print("\n⏹️ 用户中断操作")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 致命错误: {str(e)}")
        sys.exit(1)
