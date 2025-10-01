#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV直播源智能处理系统
修复版 - 与GitHub Actions工作流完全兼容
"""

import requests
import pandas as pd
import re
import os
import subprocess
import time
import stat
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
import argparse

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("iptv_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IPTVProcessor:
    """IPTV直播源智能处理器 - 修复版"""
    
    def __init__(self, config_file: str = None):
        """初始化配置"""
        self.config = self._load_config(config_file)
        self.sources = self._init_sources()
        self.template = self._load_template()
        self._setup_directories()
        
    def _load_config(self, config_file: str = None) -> Dict[str, Any]:
        """加载配置，支持环境变量覆盖"""
        base_config = {
            # 路径配置
            'template_file': os.getenv('TEMPLATE_FILE', 'demo.txt'),
            'output_dir': os.getenv('OUTPUT_DIR', './output'),
            'log_file': 'iptv_processor.log',
            
            # 功能参数
            'max_sources_per_channel': int(os.getenv('MAX_SOURCES_PER_CHANNEL', '8')),
            'min_stream_speed': float(os.getenv('MIN_STREAM_SPEED', '0.8')),
            'test_duration': int(os.getenv('TEST_DURATION', '5')),
            
            # 网络参数
            'request_timeout': int(os.getenv('REQUEST_TIMEOUT', '10')),
            'max_redirects': int(os.getenv('MAX_REDIRECTS', '3')),
            'retry_times': int(os.getenv('RETRY_TIMES', '2')),
            'retry_delay': int(os.getenv('RETRY_DELAY', '2')),
            
            # 性能参数
            'max_fetch_threads': int(os.getenv('MAX_FETCH_THREADS', '15')),
            'max_test_threads': int(os.getenv('MAX_TEST_THREADS', '20')),
            'speed_test_timeout': int(os.getenv('SPEED_TEST_TIMEOUT', '8')),
            
            # 用户代理
            'user_agent': 'Mozilla/5.0 (compatible; IPTV-Processor/2.0)'
        }
        
        # 从配置文件加载（如果存在）
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    file_config = json.load(f)
                    base_config.update(file_config)
            except Exception as e:
                logger.warning(f"配置文件加载失败: {e}")
                
        return base_config
    
    def _setup_directories(self):
        """创建必要的目录结构"""
        os.makedirs(self.config['output_dir'], exist_ok=True)
        # 设置目录权限
        try:
            os.chmod(self.config['output_dir'], 0o755)
        except:
            pass  # 权限设置失败不影响主要功能
    
    def _init_sources(self) -> List[str]:
        """初始化直播源列表 - 增强稳定性"""
        base_sources = [
            "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
            "https://live.zbds.top/tv/iptv6.txt", 
            "https://live.zbds.top/tv/iptv4.txt",
            "http://home.jundie.top/Cat/tv/live.txt"
        ]
        
        # 备用镜像源
        backup_sources = [
            "https://ghproxy.com/https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
            "https://mirror.ghproxy.com/https://raw.githubusercontent.com/IPTV-World/IPTV-World/master/cn.m3u"
        ]
        
        return base_sources + backup_sources
    
    def _load_template(self) -> Optional[Dict[str, Any]]:
        """安全加载频道模板"""
        template_path = self.config['template_file']
        if not os.path.exists(template_path):
            logger.warning(f"模板文件不存在: {template_path}")
            return None
            
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()
                return self._parse_template(content)
        except Exception as e:
            logger.error(f"模板加载失败: {e}")
            return None
    
    def _parse_template(self, content: str) -> Dict[str, Any]:
        """解析模板内容 - 增强容错性"""
        template = {'channels': [], 'categories': {}, 'order': []}
        current_category = None
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            if "#genre#" in line:
                current_category = line.replace("#genre#", "").strip()
            elif ',' in line:
                try:
                    channel = line.split(',')[0].strip()
                    if channel:  # 确保频道名不为空
                        template['channels'].append(channel)
                        template['order'].append(channel)
                        if current_category:
                            template['categories'][channel] = current_category
                except IndexError:
                    continue  # 跳过格式错误的行
                    
        return template
    
    def _fetch_with_retry(self, url: str, retry: int = 0) -> Optional[str]:
        """增强的带重试机制的源获取"""
        try:
            headers = {
                'User-Agent': self.config['user_agent'],
                'Accept': 'text/plain, */*'
            }
            
            response = requests.get(
                url,
                headers=headers,
                timeout=self.config['request_timeout'],
                allow_redirects=True,
                verify=True  # 启用SSL验证
            )
            response.raise_for_status()
            
            # 验证内容类型
            content_type = response.headers.get('content-type', '')
            if 'text/plain' not in content_type and 'application/octet-stream' not in content_type:
                logger.warning(f"异常内容类型: {content_type} for {url}")
                
            return response.text
            
        except requests.exceptions.SSLError:
            return self._handle_ssl_error(url, retry)
        except requests.exceptions.ProxyError:
            return self._handle_proxy_error(url, retry)
        except requests.exceptions.RequestException as e:
            return self._handle_network_error(url, retry, e)
        except Exception as e:
            logger.error(f"未知错误 [{url}]: {e}")
            return None
    
    def _handle_ssl_error(self, url: str, retry: int) -> Optional[str]:
        """处理SSL错误"""
        if retry < self.config['retry_times']:
            logger.warning(f"SSL错误，尝试降级HTTPS→HTTP: {url}")
            insecure_url = url.replace('https://', 'http://')
            return self._fetch_with_retry(insecure_url, retry + 1)
        return None
    
    def _handle_proxy_error(self, url: str, retry: int) -> Optional[str]:
        """处理代理错误"""
        if retry == 0:
            logger.warning("检测到代理问题，尝试绕过代理...")
            # 临时禁用代理
            session = requests.Session()
            session.trust_env = False
            try:
                response = session.get(url, timeout=self.config['request_timeout'])
                return response.text
            except:
                pass
        return None
    
    def _handle_network_error(self, url: str, retry: int, error: Exception) -> Optional[str]:
        """处理网络错误"""
        if retry < self.config['retry_times']:
            delay = self.config['retry_delay'] * (retry + 1)
            logger.warning(f"请求失败 [{url}]，{delay}秒后重试... 错误: {error}")
            time.sleep(delay)
            return self._fetch_with_retry(url, retry + 1)
        logger.error(f"最终请求失败 [{url}]: {error}")
        return None
    
    def fetch_all_sources(self) -> Optional[str]:
        """并发获取所有直播源 - 增强稳定性"""
        logger.info(f"开始从 {len(self.sources)} 个源抓取数据...")
        
        successful_sources = []
        with ThreadPoolExecutor(max_workers=self.config['max_fetch_threads']) as executor:
            futures = {executor.submit(self._fetch_with_retry, url): url for url in self.sources}
            
            for future in as_completed(futures):
                url = futures[future]
                try:
                    content = future.result()
                    if content:
                        successful_sources.append(content)
                        logger.info(f"✅ 成功获取: {url}")
                    else:
                        logger.warning(f"❌ 获取失败: {url}")
                except Exception as e:
                    logger.error(f"🚨 处理异常 [{url}]: {e}")
        
        if successful_sources:
            combined_content = "\n".join(successful_sources)
            logger.info(f"成功获取 {len(successful_sources)}/{len(self.sources)} 个源")
            return combined_content
        else:
            logger.error("所有源获取失败")
            return None
    
    def parse_streams(self, content: str) -> List[Dict[str, str]]:
        """解析直播源内容 - 增强容错性"""
        streams = []
        
        if not content or not content.strip():
            logger.warning("内容为空，无法解析")
            return streams
        
        lines = content.splitlines()
        logger.info(f"开始解析 {len(lines)} 行内容...")
        
        # 自动识别格式
        if content.startswith("#EXTM3U"):
            streams.extend(self._parse_m3u_content(content))
        else:
            streams.extend(self._parse_txt_content(content))
            
        logger.info(f"解析完成，共找到 {len(streams)} 个流")
        return streams
    
    def _parse_m3u_content(self, content: str) -> List[Dict[str, str]]:
        """解析M3U格式内容"""
        streams = []
        current_channel = None
        
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
                
            if line.startswith("#EXTINF"):
                # 多种格式支持
                name_match = re.search(r'tvg-name="([^"]+)"', line)
                if name_match:
                    current_channel = name_match.group(1)
                else:
                    # 备用解析方式
                    parts = line.split(',', 1)
                    if len(parts) > 1:
                        current_channel = parts[1].strip()
            elif line.startswith(('http://', 'https://', 'rtmp://', 'rtsp://')):
                if current_channel:
                    streams.append({
                        'channel': current_channel,
                        'url': line
                    })
                    current_channel = None
                    
        return streams
    
    def _parse_txt_content(self, content: str) -> List[Dict[str, str]]:
        """解析TXT格式内容"""
        streams = []
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            # 支持多种分隔符
            for sep in [',', '|', ';']:
                if sep in line:
                    parts = line.split(sep, 1)
                    if len(parts) == 2 and parts[1].startswith(('http://', 'https://')):
                        streams.append({
                            'channel': parts[0].strip(),
                            'url': parts[1].strip()
                        })
                        break
                        
        return streams
    
    def _test_stream_quality(self, url: str) -> Optional[float]:
        """FFmpeg流质量测试 - 增强稳定性"""
        try:
            # 验证URL格式
            if not re.match(r'^https?://', url):
                return None
                
            cmd = [
                'ffmpeg',
                '-hide_banner',
                '-loglevel', 'error',
                '-i', url,
                '-t', str(self.config['test_duration']),
                '-f', 'null',
                '-',
                *self.config.get('ffmpeg_args', [])
            ]
            
            # 超时控制
            result = subprocess.run(
                cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                timeout=self.config['speed_test_timeout'],
                check=False  # 不抛出异常，手动处理
            )
            
            if result.returncode != 0:
                return None
                
            # 分析输出
            output = result.stderr.decode('utf-8', errors='ignore')
            speed_match = re.search(r'speed=\s*([\d.]+)x', output)
            
            if not speed_match:
                return None
                
            speed = float(speed_match.group(1))
            return speed if speed >= self.config['min_stream_speed'] else None
            
        except subprocess.TimeoutExpired:
            logger.debug(f"测速超时: {url}")
            return None
        except Exception as e:
            logger.debug(f"测速失败 [{url}]: {e}")
            return None
    
    def optimize_streams(self, streams: List[Dict[str, str]]) -> pd.DataFrame:
        """优化直播源数据 - 增强性能"""
        if not streams:
            logger.warning("没有可处理的流数据")
            return pd.DataFrame(columns=['channel', 'url'])
            
        logger.info("开始优化直播源数据...")
        
        # 转换为DataFrame并清洗
        df = pd.DataFrame(streams)
        initial_count = len(df)
        
        # 数据清洗
        df = df.dropna()
        df = df.drop_duplicates(subset=['channel', 'url'])
        logger.info(f"去重后: {len(df)}/{initial_count} 条记录")
        
        # 模板过滤
        if self.template and 'channels' in self.template:
            before_filter = len(df)
            df = df[df['channel'].isin(self.template['channels'])]
            logger.info(f"模板过滤后: {len(df)}/{before_filter} 条记录")
        
        if len(df) == 0:
            logger.warning("过滤后无有效数据")
            return pd.DataFrame(columns=['channel', 'url'])
        
        # 分组处理
        grouped = df.groupby('channel')['url'].apply(list).reset_index()
        
        # 多线程测速优化
        def optimize_channel_sources(urls: List[str]) -> List[str]:
            if not urls:
                return []
                
            results = {}
            with ThreadPoolExecutor(max_workers=self.config['max_test_threads']) as executor:
                future_to_url = {executor.submit(self._test_stream_quality, url): url for url in urls}
                
                completed = 0
                total = len(urls)
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    results[url] = future.result()
                    completed += 1
                    if completed % 10 == 0:
                        logger.info(f"测速进度: {completed}/{total}")
            
            # 筛选并排序
            valid_urls = {url: score for url, score in results.items() if score is not None}
            sorted_urls = sorted(valid_urls.keys(), key=lambda x: valid_urls[x], reverse=True)
            
            max_sources = self.config['max_sources_per_channel']
            return sorted_urls[:max_sources]
        
        grouped['url'] = grouped['url'].apply(optimize_channel_sources)
        final_count = sum(len(urls) for urls in grouped['url'])
        logger.info(f"优化完成: {len(grouped)} 个频道, {final_count} 个有效源")
        
        return grouped
    
    def generate_outputs(self, data: pd.DataFrame) -> bool:
        """生成输出文件 - 增强稳定性"""
        try:
            output_dir = self.config['output_dir']
            
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 生成文本格式
            txt_success = self._generate_txt_output(data, output_dir)
            # 生成M3U格式
            m3u_success = self._generate_m3u_output(data, output_dir)
            
            if txt_success and m3u_success:
                logger.info("✅ 所有输出文件生成成功")
                return True
            else:
                logger.error("❌ 部分文件生成失败")
                return False
                
        except Exception as e:
            logger.error(f"文件生成异常: {e}")
            return False
    
    def _generate_txt_output(self, data: pd.DataFrame, output_dir: str) -> bool:
        """生成文本格式输出"""
        try:
            txt_path = os.path.join(output_dir, 'iptv.txt')
            
            with open(txt_path, 'w', encoding='utf-8') as f:
                current_category = None
                
                for _, row in data.iterrows():
                    channel, urls = row['channel'], row['url']
                    
                    # 分类标题
                    if self.template and 'categories' in self.template:
                        category = self.template['categories'].get(channel)
                        if category and category != current_category:
                            f.write(f"\n{category},#genre#\n")
                            current_category = category
                    
                    # 写入频道
                    for url in urls:
                        f.write(f"{channel},{url}\n")
            
            # 设置文件权限
            try:
                os.chmod(txt_path, 0o644)
            except:
                pass
                
            logger.info(f"📄 文本文件已生成: {txt_path}")
            return True
            
        except Exception as e:
            logger.error(f"文本文件生成失败: {e}")
            return False
    
    def _generate_m3u_output(self, data: pd.DataFrame, output_dir: str) -> bool:
        """生成M3U格式输出"""
        try:
            m3u_path = os.path.join(output_dir, 'iptv.m3u')
            
            with open(m3u_path, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                current_category = None
                
                for _, row in data.iterrows():
                    channel, urls = row['channel'], row['url']
                    
                    # 分类标题
                    if self.template and 'categories' in self.template:
                        category = self.template['categories'].get(channel)
                        if category and category != current_category:
                            f.write(f'#EXTINF:-1 tvg-name="{category}" group-title="{category}",{category}\n')
                            current_category = category
                    
                    # 写入频道
                    for url in urls:
                        f.write(f'#EXTINF:-1 tvg-name="{channel}",{channel}\n{url}\n')
            
            # 设置文件权限
            try:
                os.chmod(m3u_path, 0o644)
            except:
                pass
                
            logger.info(f"🎵 M3U文件已生成: {m3u_path}")
            return True
            
        except Exception as e:
            logger.error(f"M3U文件生成失败: {e}")
            return False
    
    def run(self) -> bool:
        """主执行流程 - 返回执行状态"""
        logger.info("🚀 IPTV处理器开始运行")
        
        try:
            # 第一阶段：数据采集
            content = self.fetch_all_sources()
            if not content:
                logger.error("数据采集失败")
                return False
            
            # 第二阶段：数据处理
            streams = self.parse_streams(content)
            if not streams:
                logger.error("数据解析失败")
                return False
                
            optimized_data = self.optimize_streams(streams)
            if len(optimized_data) == 0:
                logger.error("数据优化后无有效结果")
                return False
            
            # 第三阶段：结果输出
            success = self.generate_outputs(optimized_data)
            if success:
                logger.info("🎉 处理流程完成")
            else:
                logger.error("处理流程失败")
                
            return success
            
        except KeyboardInterrupt:
            logger.warning("用户中断操作")
            return False
        except Exception as e:
            logger.critical(f"未处理的异常: {e}", exc_info=True)
            return False

def main():
    """命令行入口点"""
    parser = argparse.ArgumentParser(description='IPTV直播源处理器')
    parser.add_argument('--config', '-c', help='配置文件路径')
    parser.add_argument('--output-dir', '-o', help='输出目录')
    parser.add_argument('--max-threads', type=int, help='最大线程数')
    parser.add_argument('--timeout', type=int, help='超时时间')
    
    args = parser.parse_args()
    
    # 设置环境变量（如果提供了命令行参数）
    if args.output_dir:
        os.environ['OUTPUT_DIR'] = args.output_dir
    if args.max_threads:
        os.environ['MAX_FETCH_THREADS'] = str(args.max_threads)
    if args.timeout:
        os.environ['REQUEST_TIMEOUT'] = str(args.timeout)
    
    processor = IPTVProcessor(args.config)
    success = processor.run()
    
    # 返回适当的退出码
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
