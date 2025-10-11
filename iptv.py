#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IPTV直播源抓取与测速工具
功能：从多个源获取直播源，进行测速筛选，生成多种格式的输出文件
作者：AI助手
版本：2.0
"""

import requests
import pandas as pd
import re
import os
import time
import concurrent.futures
import logging
from typing import List, Dict, Optional, Tuple, Set, Any, Union
from urllib.parse import urlparse
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json
import argparse
from collections import defaultdict
import threading

@dataclass
class TestResult:
    """测速结果数据类"""
    url: str                    # 测试的URL地址
    speed: Optional[float]      # 测速结果(KB/s)，None表示测试失败
    error: Optional[str]        # 错误信息，成功时为None
    response_time: float        # 响应时间(秒)
    status_code: Optional[int]  # HTTP状态码
    content_type: Optional[str] # 内容类型
    success: bool              # 测试是否成功

class IPTVConfig:
    """IPTV工具配置类"""
    
    def __init__(self):
        # 网络配置
        self.timeout = 10                    # 请求超时时间(秒)
        self.max_workers = 6               # 最大并发线程数
        self.test_size_kb = 1024            # 测速数据大小(KB)，增加数据量提高准确性
        self.retry_times = 2               # 重试次数
        self.request_delay = 0.3           # 请求间延迟(秒)，避免请求过快
        
        # 测速配置
        self.min_speed_threshold = 500      # 最小速度阈值(KB/s)，低于此值的源将被丢弃
        self.max_test_per_channel = 30     # 每个频道最大测试源数
        self.keep_best_sources = 8         # 每个频道保留最佳源数量
        self.speed_test_duration = 10       # 测速最大持续时间(秒)
        
        # 数据源配置 - 多个直播源URL
        self.source_urls = [
            "https://ghfast.top/raw.githubusercontent.com/Supprise0901/TVBox_live/main/live.txt",
            "https://gh-proxy.com/https://raw.githubusercontent.com/wwb521/live/main/tv.m3u",
            "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv4/result.m3u",  
            "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/cn.m3u",
            "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
            "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.txt",
            "https://gh-proxy.com/https://raw.githubusercontent.com/develop202/migu_video/refs/heads/main/interface.txt",
            "http://47.120.41.246:8899/zb.txt",
        ]
        
        # 文件路径配置
        self.base_dir = Path(__file__).parent  # 基础目录
        self.template_file = self.base_dir / "demo.txt"  # 模板文件路径
        self.cache_file = self.base_dir / "cache.json"   # 缓存文件路径
        
        # 输出文件配置
        self.output_files = {
            'txt': self.base_dir / "iptv.txt",           # TXT格式输出
            'm3u': self.base_dir / "iptv.m3u",           # M3U格式输出
            'log': self.base_dir / "process.log",        # 处理日志
            'report': self.base_dir / "speed_report.txt", # 测速报告
            'json': self.base_dir / "iptv_data.json"     # JSON格式数据
        }
        
        # 频道分类配置 - 用于自动分类频道
        self.channel_categories = {
            "央视频道,#genre#": ["CCTV", "央视", "Cctv", "cctv"],  # 央视相关频道
            "高清频道,#genre#": ["高清", "HD", "hd", "4K", "4k"],  # 高清频道
            "卫视频道,#genre#": ["卫视", "湖南", "浙江", "江苏", "东方", "北京", "天津", "河北", "山东", "安徽"],  # 卫视频道
            "地方频道,#genre#": ["重庆", "广东", "深圳", "南方", "广州", "四川", "福建", "湖北", "辽宁"],  # 地方频道
            "港澳频道,#genre#": ["凤凰", "翡翠", "明珠", "澳门", "香港", "港澳"],  # 港澳频道
            "影视频道,#genre#": ["电影", "影院", "剧场", "影视"],  # 影视相关频道
            "体育频道,#genre#": ["体育", "足球", "篮球", "奥运", "NBA", "CBA"],  # 体育频道
            "其他频道,#genre#": []  # 未分类频道
        }
        
        # HTTP请求头配置
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }

class IPTVTool:
    """IPTV直播源抓取与测速工具主类"""
    
    def __init__(self, config: Optional[IPTVConfig] = None):
        """
        初始化IPTV工具
        
        Args:
            config: 配置对象，如果为None则使用默认配置
        """
        self.config = config or IPTVConfig()  # 使用传入配置或默认配置
        
        # 请求会话配置 - 复用连接提高效率
        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
        
        # 正则表达式预编译 - 提高解析效率
        self.ipv4_pattern = re.compile(r'^http://(\d{1,3}\.){3}\d{1,3}')  # IPv4地址匹配
        self.ipv6_pattern = re.compile(r'^http://\[([a-fA-F0-9:]+)\]')     # IPv6地址匹配
        self.channel_pattern = re.compile(r'^([^,#]+)')                    # 频道名称匹配
        self.extinf_pattern = re.compile(r'#EXTINF:.*?,(.+)', re.IGNORECASE)  # M3U格式频道信息
        self.tvg_name_pattern = re.compile(r'tvg-name="([^"]*)"', re.IGNORECASE)  # M3U频道名
        self.tvg_logo_pattern = re.compile(r'tvg-logo="([^"]*)"', re.IGNORECASE)  # M3U台标
        self.group_title_pattern = re.compile(r'group-title="([^"]*)"', re.IGNORECASE)  # M3U分组
        
        # 状态变量
        self.valid_channels = self.load_template_channels()  # 有效频道列表
        self.url_cache = {}              # URL测速缓存，避免重复测速
        self.processed_count = 0         # 已处理URL计数
        self.lock = threading.Lock()     # 线程锁，用于并发安全
        
        # 初始化系统
        self.setup_logging()    # 设置日志系统
        self.setup_directories()  # 创建必要目录

    def setup_logging(self):
        """初始化日志系统，创建日志文件并设置格式"""
        # 创建所有输出文件的目录
        for file_path in self.config.output_files.values():
            file_path.parent.mkdir(exist_ok=True)
            
        # 初始化日志文件，写入头部信息
        with open(self.config.output_files['log'], 'w', encoding='utf-8') as f:
            f.write(f"IPTV Tool Process Log - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*60 + "\n")

    def setup_directories(self):
        """创建必要的文件目录"""
        self.config.base_dir.mkdir(exist_ok=True)

    def log(self, message: str, level="INFO", console_print=True):
        """
        记录日志到文件和控制台
        
        Args:
            message: 日志消息
            level: 日志级别 (INFO, SUCCESS, WARNING, ERROR, DEBUG)
            console_print: 是否在控制台显示
        """
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')  # 时间戳
        log_entry = f"[{timestamp}] [{level}] {message}\n"
        
        # 写入日志文件
        with open(self.config.output_files['log'], 'a', encoding='utf-8') as f:
            f.write(log_entry)
        
        # 控制台输出（带颜色）
        if console_print:
            # 定义不同日志级别的颜色
            level_color = {
                "INFO": "\033[94m",    # 蓝色
                "SUCCESS": "\033[92m", # 绿色
                "WARNING": "\033[93m", # 黄色
                "ERROR": "\033[91m",   # 红色
                "DEBUG": "\033[90m"    # 灰色
            }
            color = level_color.get(level, "\033[0m")  # 获取颜色，默认无色
            reset = "\033[0m"  # 重置颜色
            print(f"{color}[{level}] {message}{reset}")

    def load_template_channels(self) -> Set[str]:
        """
        加载模板文件中的有效频道列表
        
        Returns:
            Set[str]: 频道名称集合
        """
        channels = set()  # 使用集合避免重复
        if not self.config.template_file.exists():
            self.log(f"模板文件 {self.config.template_file} 不存在，将处理所有频道", "WARNING")
            return channels
        
        try:
            # 读取模板文件
            with open(self.config.template_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()  # 去除首尾空白
                    if line and not line.startswith('#'):  # 跳过空行和注释行
                        if match := self.channel_pattern.match(line):
                            channel_name = match.group(1).strip()  # 提取频道名称
                            channels.add(channel_name)  # 添加到集合
            self.log(f"从模板加载频道 {len(channels)} 个", "SUCCESS")
        except Exception as e:
            self.log(f"加载模板文件错误: {str(e)}", "ERROR")
        
        return channels

    # ==================== 数据获取与处理 ====================
    
    def fetch_single_source(self, url: str) -> Tuple[str, Optional[str]]:
        """
        抓取单个源的数据
        
        Args:
            url: 数据源URL
            
        Returns:
            Tuple[str, Optional[str]]: (URL, 内容) 或 (URL, None) 如果失败
        """
        self.log(f"抓取源: {self._extract_domain(url)}")
        
        # 重试机制
        for attempt in range(self.config.retry_times + 1):
            try:
                # 添加延迟避免请求过快
                if attempt > 0:
                    time.sleep(1)
                    
                # 发送HTTP请求
                response = self.session.get(url, timeout=self.config.timeout)
                response.raise_for_status()  # 检查HTTP状态码
                
                # 验证内容有效性
                content = response.text
                if self.validate_content(content):
                    self.log(f"成功抓取: {self._extract_domain(url)} (大小: {len(content)} 字符)", "SUCCESS")
                    return url, content
                else:
                    raise ValueError("内容格式无效")
                    
            except Exception as e:
                if attempt < self.config.retry_times:
                    self.log(f"第{attempt+1}次尝试失败 {self._extract_domain(url)}: {str(e)}，重试...", "WARNING")
                else:
                    self.log(f"抓取失败 {self._extract_domain(url)}: {str(e)}", "ERROR")
        return url, None

    def validate_content(self, content: str) -> bool:
        """
        验证内容是否为有效的直播源格式
        
        Args:
            content: 要验证的内容
            
        Returns:
            bool: 是否为有效直播源
        """
        if not content or len(content.strip()) < 10:
            return False  # 内容为空或太短
        
        # 检查是否包含直播源特征模式
        patterns = [
            r'http://[^\s]+',  # HTTP URL
            r'#EXTINF',        # M3U格式标记
            r',http',          # TXT格式分隔符
            r'\.m3u8?',        # M3U8文件
            r'\.ts'            # TS流
        ]
        # 统计匹配的模式数量
        valid_patterns = sum(1 for pattern in patterns if re.search(pattern, content, re.IGNORECASE))
        return valid_patterns >= 2  # 至少匹配2个模式认为是有效内容

    def fetch_streams(self) -> Optional[str]:
        """
        从所有源URL并发抓取直播源
        
        Returns:
            Optional[str]: 合并后的内容，失败返回None
        """
        contents = []  # 存储成功获取的内容
        successful_sources = 0  # 成功源计数
        
        # 使用线程池并发抓取
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(3, len(self.config.source_urls))  # 限制最大线程数
        ) as executor:
            # 提交所有抓取任务
            future_to_url = {
                executor.submit(self.fetch_single_source, url): url 
                for url in self.config.source_urls
            }
            
            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_url):
                url, content = future.result()
                if content:
                    contents.append(content)
                    successful_sources += 1
        
        # 记录抓取结果
        self.log(f"成功抓取 {successful_sources}/{len(self.config.source_urls)} 个数据源", 
                "SUCCESS" if successful_sources > 0 else "ERROR")
        
        return "\n".join(contents) if contents else None  # 合并所有内容

    def parse_content(self, content: str) -> pd.DataFrame:
        """
        解析直播源内容为DataFrame
        
        Args:
            content: 直播源内容
            
        Returns:
            pd.DataFrame: 解析后的直播源数据
        """
        streams = []  # 存储解析后的流数据
        
        # 检测格式并选择相应的解析方法
        if content.startswith("#EXTM3U"):
            streams.extend(self._parse_m3u_content(content))  # M3U格式解析
        else:
            streams.extend(self._parse_txt_content(content))  # TXT格式解析
        
        # 检查是否解析到数据
        if not streams:
            self.log("未解析到任何直播源", "WARNING")
            return pd.DataFrame(columns=['program_name', 'stream_url', 'tvg_logo', 'group_title'])
        
        # 创建DataFrame
        df = pd.DataFrame(streams)
        
        # 过滤和去重处理
        initial_count = len(df)
        if self.valid_channels:
            # 根据模板过滤频道
            df = df[df['program_name'].isin(self.valid_channels)]
            filtered_count = initial_count - len(df)
            if filtered_count > 0:
                self.log(f"根据模板过滤掉 {filtered_count} 个频道", "INFO")
        
        # 去重处理
        df = self.deduplicate_streams(df)
        self.log(f"解析到 {len(df)} 个有效直播源", "SUCCESS")
        
        return df

    def _parse_m3u_content(self, content: str) -> List[Dict[str, str]]:
        """
        解析M3U格式内容
        
        Args:
            content: M3U格式内容
            
        Returns:
            List[Dict[str, str]]: 解析后的流数据列表
        """
        streams = []  # 存储解析结果
        lines = content.splitlines()  # 按行分割
        current_program = None  # 当前节目名称
        current_logo = None     # 当前台标URL
        current_group = None    # 当前分组
        
        # 遍历所有行
        for i, line in enumerate(lines):
            line = line.strip()  # 去除空白
            if line.startswith("#EXTINF"):
                # 解析EXTINF行，提取节目信息
                program_name = self.extinf_pattern.search(line)
                if program_name:
                    current_program = program_name.group(1).strip()
                
                # 优先使用tvg-name作为节目名称
                tvg_name = self.tvg_name_pattern.search(line)
                if tvg_name and tvg_name.group(1).strip():
                    current_program = tvg_name.group(1).strip()
                
                # 提取台标和分组信息
                logo_match = self.tvg_logo_pattern.search(line)
                current_logo = logo_match.group(1) if logo_match else ""
                
                group_match = self.group_title_pattern.search(line)
                current_group = group_match.group(1) if group_match else ""
                
            elif line.startswith(("http://", "https://")) and current_program:
                # 遇到URL行，与前面的EXTINF信息组合
                streams.append({
                    "program_name": current_program,
                    "stream_url": line,
                    "tvg_logo": current_logo or "",
                    "group_title": current_group or ""
                })
                # 重置当前信息
                current_program = None
                current_logo = None
                current_group = None
        
        return streams

    def _parse_txt_content(self, content: str) -> List[Dict[str, str]]:
        """
        解析TXT格式内容
        
        Args:
            content: TXT格式内容
            
        Returns:
            List[Dict[str, str]]: 解析后的流数据列表
        """
        streams = []
        
        # 逐行解析
        for line_num, line in enumerate(content.splitlines(), 1):
            line = line.strip()
            if not line or line.startswith('#'):  # 跳过空行和注释
                continue
                
            # 匹配 "频道名称,http://url" 格式
            if match := re.match(r"^([^,]+?)\s*,\s*(http.+)$", line):
                program_name = match.group(1).strip()
                stream_url = match.group(2).strip()
                
                # 清理URL参数中的额外信息（如注释）
                stream_url = re.sub(r'\s+#.*$', '', stream_url)
                
                streams.append({
                    "program_name": program_name,
                    "stream_url": stream_url,
                    "tvg_logo": "",
                    "group_title": ""
                })
        
        return streams

    def deduplicate_streams(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        去重直播源，优先保留M3U格式的源
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 去重后的DataFrame
        """
        # 计算URL的哈希值用于去重
        def get_url_key(url):
            # 移除参数进行基础去重，只比较基础URL
            base_url = url.split('?')[0].split('#')[0]
            return hashlib.md5(base_url.encode()).hexdigest()
        
        df['url_key'] = df['stream_url'].apply(get_url_key)
        
        # 优先保留有logo和group信息的源（通常是M3U格式，质量更好）
        df['priority'] = df.apply(
            lambda x: 2 if x['tvg_logo'] or x['group_title'] else 1, 
            axis=1
        )
        
        # 按优先级排序并去重，保留优先级高的
        df = df.sort_values('priority', ascending=False)
        df = df.drop_duplicates(subset=['program_name', 'url_key'], keep='first')
        
        # 清理临时列
        return df.drop(['url_key', 'priority'], axis=1)

    def organize_streams(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        整理直播源数据，按频道分组
        
        Args:
            df: 解析后的直播源数据
            
        Returns:
            pd.DataFrame: 分组整理后的数据
        """
        # 按频道名称分组，聚合所有URL
        grouped = df.groupby('program_name')['stream_url'].apply(list).reset_index()
        
        # 统计每个频道的源数量
        source_counts = grouped['stream_url'].apply(len)
        
        # 记录统计信息
        self.log(f"频道源数量统计: 平均{source_counts.mean():.1f}, 最多{source_counts.max()}, 最少{source_counts.min()}", "INFO")
        
        # 显示源数量分布详情
        count_distribution = source_counts.value_counts().sort_index()
        for count, freq in count_distribution.items():
            self.log(f"  {count}个源: {freq}个频道", "DEBUG")
        
        return grouped

    # ==================== 测速功能 ====================
    
    def test_single_url(self, url: str) -> TestResult:
        """
        测试单个URL的速度和质量
        
        Args:
            url: 要测试的URL
            
        Returns:
            TestResult: 测试结果
        """
        start_time = time.time()  # 开始时间
        
        # 重试机制
        for attempt in range(self.config.retry_times + 1):
            try:
                # 添加请求延迟，避免过快请求
                if attempt > 0:
                    time.sleep(0.5)
                
                # 检查缓存，避免重复测速
                cache_key = hashlib.md5(url.encode()).hexdigest()
                if cache_key in self.url_cache:
                    cached_result = self.url_cache[cache_key]
                    # 5分钟缓存有效期
                    if time.time() - cached_result['timestamp'] < 300:
                        self.log(f"使用缓存结果: {self._extract_domain(url)}", "DEBUG")
                        return cached_result['result']
                
                # 开始测试
                test_start = time.time()
                with self.session.get(
                    url, 
                    timeout=self.config.timeout, 
                    stream=True  # 流式传输，避免一次性加载大文件
                ) as response:
                    response_time = time.time() - test_start  # 响应时间
                    
                    # 检查HTTP状态和内容类型
                    status_code = response.status_code
                    content_type = response.headers.get('content-type', '')
                    
                    if status_code != 200:
                        return TestResult(
                            url, None, f"HTTP {status_code}", 
                            response_time, status_code, content_type, False
                        )
                    
                    # 测速：下载指定大小的数据计算速度
                    content_length = 0
                    chunk_count = 0
                    start_download = time.time()
                    
                    # 分块读取数据
                    for chunk in response.iter_content(chunk_size=8192):
                        content_length += len(chunk)
                        chunk_count += 1
                        
                        # 达到测试数据量或超时则停止
                        if (content_length >= self.config.test_size or 
                            time.time() - start_download > self.config.speed_test_duration):
                            break
                    
                    download_time = time.time() - start_download
                    
                    # 计算速度（至少1KB数据才认为有效）
                    if content_length > 1024:
                        speed = content_length / download_time / 1024  # 转换为KB/s
                        
                        result = TestResult(
                            url, speed, None, response_time, 
                            status_code, content_type, True
                        )
                        
                        # 缓存成功结果
                        self.url_cache[cache_key] = {
                            'result': result,
                            'timestamp': time.time()
                        }
                        
                        return result
                    else:
                        return TestResult(
                            url, 0, "数据量不足", response_time,
                            status_code, content_type, False
                        )
                        
            except requests.exceptions.Timeout:
                error = "请求超时"
            except requests.exceptions.SSLError:
                error = "SSL证书错误"
            except requests.exceptions.ConnectionError:
                error = "连接失败"
            except requests.exceptions.HTTPError as e:
                error = f"HTTP错误 {e.response.status_code}"
            except Exception as e:
                error = f"未知错误: {str(e)}"
        
        # 所有重试都失败
        return TestResult(
            url, None, error, time.time() - start_time,
            None, None, False
        )

    def test_urls_concurrently(self, urls: List[str]) -> List[TestResult]:
        """
        并发测试URL列表
        
        Args:
            urls: 要测试的URL列表
            
        Returns:
            List[TestResult]: 测试结果列表
        """
        results = []
        total = len(urls)
        
        # 使用线程池并发测试
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # 提交所有测试任务
            future_to_url = {executor.submit(self.test_single_url, url): url for url in urls}
            
            # 处理完成的任务并显示进度
            for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
                result = future.result()
                results.append(result)
                
                # 更新进度（线程安全）
                with self.lock:
                    self.processed_count += 1
                    # 每5个或最后一个显示进度
                    if i % 5 == 0 or i == total:
                        self.log(f"测速进度: {i}/{total} ({i/total*100:.1f}%)", "INFO")
        
        return results

    def test_all_channels(self, grouped_streams: pd.DataFrame) -> Dict[str, List[Tuple[str, float]]]:
        """
        测试所有频道并保留最佳源
        
        Args:
            grouped_streams: 分组后的直播源数据
            
        Returns:
            Dict[str, List[Tuple[str, float]]]: 频道到最佳源列表的映射
        """
        results = {}  # 存储结果
        total_channels = len(grouped_streams)  # 总频道数
        successful_channels = 0  # 成功频道计数
        
        self.log(f"开始测速 {total_channels} 个频道", "INFO")
        self.log(f"每个频道测试最多{self.config.max_test_per_channel}个源，保留最佳{self.config.keep_best_sources}个", "INFO")
        
        self.processed_count = 0  # 重置计数器
        
        # 遍历所有频道
        for idx, (_, row) in enumerate(grouped_streams.iterrows(), 1):
            channel = row['program_name']
            urls = row['stream_url'][:self.config.max_test_per_channel]  # 限制测试数量
            
            self.log(f"[{idx}/{total_channels}] 测试频道: {channel} ({len(urls)}个源)")
            
            # 并发测试该频道的所有URL
            test_results = self.test_urls_concurrently(urls)
            valid_streams = []  # 有效源列表
            
            # 处理测试结果
            for result in test_results:
                # 检查是否成功且达到速度阈值
                if result.success and result.speed and result.speed >= self.config.min_speed_threshold:
                    valid_streams.append((result.url, result.speed))
                    status = "✓" if result.speed > 200 else "⚠️"  # 速度状态图标
                    speed_quality = self.get_speed_quality(result.speed)  # 速度质量评级
                    response_info = f"{result.response_time:.2f}s"  # 响应时间
                    self.log(f"    {status} {self._extract_domain(result.url)}: {result.speed:.1f} KB/s ({speed_quality}) [{response_info}]")
                else:
                    error_info = result.error or "速度过低"  # 错误信息
                    self.log(f"    ✗ {self._extract_domain(result.url)}: {error_info}")
            
            # 按速度排序并保留最佳源
            valid_streams.sort(key=lambda x: x[1], reverse=True)  # 降序排序
            results[channel] = valid_streams[:self.config.keep_best_sources]
            
            # 记录频道测试结果
            if results[channel]:
                successful_channels += 1
                best_speed = results[channel][0][1]  # 最佳速度
                self.log(f"    ✅ 最佳源: {best_speed:.1f} KB/s (保留{len(results[channel])}个)", "SUCCESS")
            else:
                self.log("    ❌ 无有效源", "WARNING")
        
        # 最终统计
        self.log(f"测速完成: {successful_channels}/{total_channels} 个频道有有效源", 
                "SUCCESS" if successful_channels > 0 else "ERROR")
        
        return results

    # ==================== 结果输出 ====================
    
    def generate_output_files(self, speed_results: Dict[str, List[Tuple[str, float]]]):
        """生成所有输出文件"""
        self.generate_txt_file(speed_results)    # 生成TXT文件
        self.generate_m3u_file(speed_results)    # 生成M3U文件
        self.generate_json_file(speed_results)   # 生成JSON文件
        self.generate_report(speed_results)      # 生成测速报告

    def generate_txt_file(self, results: Dict[str, List[Tuple[str, float]]]):
        """
        生成TXT格式文件
        
        Args:
            results: 测速结果字典
        """
        # 初始化分类字典
        categorized = {cat: [] for cat in self.config.channel_categories}
        
        # 按分类组织频道
        for channel in self.get_ordered_channels(results.keys()):
            streams = results.get(channel, [])
            if not streams:
                continue
                
            matched = False
            # 匹配频道分类
            for cat, keywords in self.config.channel_categories.items():
                if any(keyword in channel for keyword in keywords):
                    # 添加格式化的频道信息
                    categorized[cat].extend(
                        f"{channel},{url} # 速度: {speed:.1f}KB/s" 
                        for url, speed in streams
                    )
                    matched = True
                    break
            
            # 未匹配的频道归为其他
            if not matched:
                categorized["其他频道,#genre#"].extend(
                    f"{channel},{url} # 速度: {speed:.1f}KB/s" 
                    for url, speed in streams
                )
        
        # 写入文件
        with open(self.config.output_files['txt'], 'w', encoding='utf-8') as f:
            for cat, items in categorized.items():
                if items:
                    f.write(f"\n{cat}\n")  # 分类标题
                    f.write("\n".join(items) + "\n")  # 频道列表
        
        total_streams = sum(len(items) for items in categorized.values())
        self.log(f"生成TXT文件: {self.config.output_files['txt']} (共 {total_streams} 个源)", "SUCCESS")

    def generate_m3u_file(self, results: Dict[str, List[Tuple[str, float]]]):
        """
        生成M3U格式文件
        
        Args:
            results: 测速结果字典
        """
        total_streams = 0
        
        with open(self.config.output_files['m3u'], 'w', encoding='utf-8') as f:
            f.write('#EXTM3U x-tvg-url=""\n')  # M3U文件头
            
            # 遍历所有频道
            for channel in self.get_ordered_channels(results.keys()):
                streams = results.get(channel, [])
                for url, speed in streams:
                    quality = self.get_speed_quality(speed)  # 速度质量
                    group = self.categorize_channel(channel)  # 频道分类
                    
                    # 写入EXTINF行
                    f.write(f'#EXTINF:-1 tvg-id="" tvg-name="{channel}" tvg-logo="" group-title="{group}",{channel} [速度: {speed:.1f}KB/s {quality}]\n')
                    f.write(f'{url}\n')  # URL行
                    total_streams += 1
        
        self.log(f"生成M3U文件: {self.config.output_files['m3u']} (共 {total_streams} 个源)", "SUCCESS")

    def generate_json_file(self, results: Dict[str, List[Tuple[str, float]]]):
        """
        生成JSON格式文件
        
        Args:
            results: 测速结果字典
        """
        # 构建数据结构
        data = {
            "metadata": {
                "generated_time": time.strftime('%Y-%m-%d %H:%M:%S'),
                "total_channels": len(results),
                "total_streams": sum(len(streams) for streams in results.values())
            },
            "channels": {}
        }
        
        # 填充频道数据
        for channel, streams in results.items():
            data["channels"][channel] = {
                "best_speed": streams[0][1] if streams else 0,  # 最佳速度
                "stream_count": len(streams),  # 源数量
                "streams": [
                    {
                        "url": url,
                        "speed": speed,
                        "quality": self.get_speed_quality(speed),  # 质量评级
                        "domain": self._extract_domain(url)  # 域名
                    }
                    for url, speed in streams
                ],
                "category": self.categorize_channel(channel)  # 分类
            }
        
        # 写入JSON文件
        with open(self.config.output_files['json'], 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)  # 美化输出
        
        self.log(f"生成JSON文件: {self.config.output_files['json']}", "SUCCESS")

    def generate_report(self, results: Dict[str, List[Tuple[str, float]]]):
        """
        生成详细测速报告
        
        Args:
            results: 测速结果字典
        """
        speed_stats = []  # 速度统计
        valid_channels = []  # 有效频道列表
        
        # 收集统计信息
        for channel, streams in results.items():
            if streams:
                best_speed = streams[0][1]  # 每个频道的最佳速度
                speed_stats.append(best_speed)
                valid_channels.append((channel, best_speed, len(streams)))
        
        # 检查是否有有效数据
        if not speed_stats:
            self.log("无有效测速结果，跳过报告生成", "WARNING")
            return
        
        # 按速度排序频道（降序）
        valid_channels.sort(key=lambda x: x[1], reverse=True)
        
        # 生成报告内容
        report_lines = [
            "="*60,
            "IPTV直播源测速报告",
            "="*60,
            f"生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"有效频道数: {len(valid_channels)}",
            f"总源数量: {sum(x[2] for x in valid_channels)}",
            f"平均速度: {sum(speed_stats)/len(speed_stats):.1f} KB/s",
            f"最快速度: {max(speed_stats):.1f} KB/s",
            f"最慢速度: {min(speed_stats):.1f} KB/s",
            f"速度中位数: {sorted(speed_stats)[len(speed_stats)//2]:.1f} KB/s",
            "\n速度分布:",
        ]
        
        # 速度分布统计
        speed_ranges = [
            (1000, "极速(>1000)"),
            (500, "优秀(500-1000)"), 
            (200, "良好(200-500)"),
            (100, "一般(100-200)"),
            (50, "较差(50-100)"),
            (0, "极差(<50)")
        ]
        
        range_counts = {}
        total = len(speed_stats)
        
        # 计算每个速度区间的频道数量
        for i, (min_speed, range_name) in enumerate(speed_ranges):
            if i == len(speed_ranges) - 1:  # 最后一个区间
                count = len([s for s in speed_stats if s <= min_speed])
            else:
                next_min = speed_ranges[i+1][0]
                count = len([s for s in speed_stats if min_speed < s <= next_min])
            range_counts[range_name] = count
            percentage = count / total * 100  # 百分比
            report_lines.append(f"  {range_name:<15} KB/s: {count:>3}个频道 ({percentage:5.1f}%)")
        
        # 添加TOP 20频道排名
        report_lines.extend(["\n频道速度排名 TOP 20:", "-"*50])
        
        for i, (channel, speed, count) in enumerate(valid_channels[:20], 1):
            quality = self.get_speed_quality(speed)
            report_lines.append(f"{i:2d}. {channel:<20} {speed:6.1f} KB/s ({quality:>4}, {count}个源)")
        
        # 如果频道多于20个，添加提示
        if len(valid_channels) > 20:
            report_lines.append(f"...(共{len(valid_channels)}个频道)")
        
        report_content = "\n".join(report_lines)
        
        # 写入报告文件
        with open(self.config.output_files['report'], 'w', encoding='utf-8') as f:
            f.write(report_content + "\n")
        
        self.log(f"生成测速报告: {self.config.output_files['report']}", "SUCCESS")
        
        # 在控制台显示摘要
        self.log("\n" + "\n".join(report_lines[:15]))

    # ==================== 辅助方法 ====================
    
    def get_ordered_channels(self, channels: List[str]) -> List[str]:
        """
        按照模板顺序排序频道列表
        
        Args:
            channels: 频道名称列表
            
        Returns:
            List[str]: 排序后的频道列表
        """
        # 如果没有模板，按字母顺序排序
        if not self.valid_channels:
            return sorted(channels)
        
        ordered = []
        # 首先添加模板中的频道（按模板顺序）
        if self.config.template_file.exists():
            with open(self.config.template_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if match := self.channel_pattern.match(line):
                            channel = match.group(1).strip()
                            if channel in channels and channel not in ordered:
                                ordered.append(channel)
        
        # 添加未在模板中的频道（按字母顺序）
        remaining_channels = [ch for ch in channels if ch not in ordered]
        ordered.extend(sorted(remaining_channels))
                
        return ordered

    def _extract_domain(self, url: str) -> str:
        """
        从URL提取域名
        
        Args:
            url: 完整URL
            
        Returns:
            str: 域名或截断的URL
        """
        try:
            netloc = urlparse(url).netloc  # 解析网络位置
            return netloc.split(':')[0]  # 移除端口号
        except:
            # 解析失败时返回截断的URL
            return url[:25] + "..." if len(url) > 25 else url

    def categorize_channel(self, channel: str) -> str:
        """
        根据频道名称分类
        
        Args:
            channel: 频道名称
            
        Returns:
            str: 分类名称
        """
        for category, keywords in self.config.channel_categories.items():
            if any(keyword in channel for keyword in keywords):
                return category.replace(",#genre#", "")  # 移除格式后缀
        return "其他频道"  # 默认分类

    def get_speed_quality(self, speed: float) -> str:
        """
        根据速度值获取质量评级
        
        Args:
            speed: 速度值(KB/s)
            
        Returns:
            str: 质量评级描述
        """
        if speed > 1000: return "极速"
        if speed > 500: return "优秀" 
        if speed > 200: return "良好"
        if speed > 100: return "一般"
        if speed > 50: return "较差"
        return "极差"

    # ==================== 主流程 ====================
    
    def run(self):
        """运行主处理流程"""
        start_time = time.time()  # 记录开始时间
        
        # 显示启动信息
        self.log("="*60)
        self.log("🎬 IPTV直播源处理工具启动")
        self.log("="*60)
        
        # 显示配置信息
        self.log(f"📋 配置参数:")
        self.log(f"   超时时间: {self.config.timeout}s")
        self.log(f"   并发线程: {self.config.max_workers}")
        self.log(f"   测速数据: {self.config.test_size_kb}KB")
        self.log(f"   重试次数: {self.config.retry_times}")
        self.log(f"   数据源数: {len(self.config.source_urls)}")
        
        # 显示模板信息
        if self.valid_channels:
            self.log(f"📺 模板频道: {len(self.valid_channels)}个")
        else:
            self.log("⚠️  未使用模板过滤，将处理所有频道", "WARNING")
        
        try:
            # 阶段1: 抓取直播源
            self.log("\n🚀 阶段1: 抓取直播源...")
            if content := self.fetch_streams():
                
                # 阶段2: 解析直播源数据
                self.log("\n🔍 阶段2: 解析直播源数据...")
                df = self.parse_content(content)
                
                # 显示频道匹配情况
                matched_channels = set(df['program_name'].unique())
                self.log(f"\n📊 频道匹配结果:")
                self.log(f"   发现频道总数: {len(matched_channels)}")
                self.log(f"   直播源总数: {len(df)}")
                
                # 模板匹配统计
                if self.valid_channels:
                    matched_template = len(matched_channels & self.valid_channels)
                    self.log(f"   匹配模板频道: {matched_template}/{len(self.valid_channels)}")
                    
                    unmatched = self.valid_channels - matched_channels
                    if unmatched:
                        self.log(f"   未匹配模板频道: {len(unmatched)}个", "WARNING")
                
                # 整理和组织数据
                grouped = self.organize_streams(df)
                self.log(f"\n📋 整理后: {len(grouped)}个频道")
                
                # 阶段3: 测速和优化
                self.log("\n⏱️  阶段3: 开始测速...")
                speed_results = self.test_all_channels(grouped)
                
                # 阶段4: 生成输出文件
                self.log("\n💾 阶段4: 生成输出文件...")
                self.generate_output_files(speed_results)
                
                # 统计最终结果
                total_streams = sum(len(streams) for streams in speed_results.values())
                valid_channel_count = len([ch for ch in speed_results if speed_results[ch]])
                
                elapsed_time = time.time() - start_time
                self.log(f"\n🎉 处理完成!")
                self.log(f"   ✅ 有效频道: {valid_channel_count}个")
                self.log(f"   📺 总直播源: {total_streams}个") 
                self.log(f"   ⏰ 总耗时: {elapsed_time:.1f}秒")
                self.log(f"   💾 输出文件:")
                # 显示所有输出文件信息
                for file_type, file_path in self.config.output_files.items():
                    if file_path.exists():
                        size = file_path.stat().st_size
                        self.log(f"      {file_type.upper()}: {file_path} ({size} bytes)")
                
            else:
                self.log("❌ 未能获取有效数据，请检查网络连接或源URL", "ERROR")
                
        except KeyboardInterrupt:
            self.log("👋 用户中断操作", "WARNING")
        except Exception as e:
            self.log(f"❌ 处理过程中发生错误: {str(e)}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")  # 记录完整堆栈跟踪

def main():
    """主函数 - 程序入口点"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='IPTV直播源抓取与测速工具')
    parser.add_argument('--timeout', type=int, default=8, help='请求超时时间(秒)')
    parser.add_argument('--workers', type=int, default=4, help='并发线程数')
    parser.add_argument('--test-size', type=int, default=128, help='测速数据大小(KB)')
    parser.add_argument('--retry', type=int, default=2, help='重试次数')
    parser.add_argument('--template', type=str, help='模板文件路径')
    parser.add_argument('--output-dir', type=str, help='输出目录路径')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    try:
        # 创建配置对象
        config = IPTVConfig()
        config.timeout = args.timeout
        config.max_workers = args.workers
        config.test_size_kb = args.test_size
        config.retry_times = args.retry
        
        # 处理自定义模板文件
        if args.template:
            config.template_file = Path(args.template)
        # 处理自定义输出目录
        if args.output_dir:
            config.base_dir = Path(args.output_dir)
            # 更新所有输出文件路径
            for key in config.output_files:
                config.output_files[key] = config.base_dir / config.output_files[key].name
        
        # 创建并运行工具
        tool = IPTVTool(config)
        tool.run()
        
    except KeyboardInterrupt:
        print("\n👋 用户中断操作")
    except Exception as e:
        print(f"❌ 程序执行错误: {e}")
        import traceback
        traceback.print_exc()  # 打印错误堆栈

if __name__ == "__main__":
    main()  # 程序入口
