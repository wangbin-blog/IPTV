#!/usr/bin/env python3
import requests
import pandas as pd
import re
import os
import time
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

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
            'timeout': 15,
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        }
        
        self.CHANNEL_CONFIG = {
            'max_sources_per_channel': 8,
            'speed_test_timeout': 5,
        }
        
        self.FILE_CONFIG = {
            'template_file': 'demo.txt',
            'output_txt': 'iptv.txt',
            'output_m3u': 'iptv.m3u',
            'temp_dir': 'temp',
        }
        
        # 初始化会话和目录
        self.session = requests.Session()
        self.session.headers.update(self.REQUEST_CONFIG['headers'])
        
        # 创建临时目录
        if not os.path.exists(self.FILE_CONFIG['temp_dir']):
            os.makedirs(self.FILE_CONFIG['temp_dir'])
        
        # 编译正则表达式
        self.ipv4_pattern = re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}')
        self.ipv6_pattern = re.compile(r'^https?://\[([a-fA-F0-9:]+)\]')
        self.extinf_pattern = re.compile(r'#EXTINF:.*?tvg-name="([^"]+)".*?,(.+)')
        self.category_pattern = re.compile(r'^(.*?),#genre#$')
        self.url_pattern = re.compile(r'https?://[^\s,]+')
        
        # 状态变量
        self.ffmpeg_available = False
        self.processed_count = 0
        self.total_count = 0

    def check_dependencies(self):
        """检查必要的依赖"""
        try:
            import requests
            import pandas
            print("✅ 基础依赖检查通过")
        except ImportError as e:
            print(f"❌ 缺少依赖: {e}")
            print("请运行: pip install requests pandas")
            return False
            
        # 检查FFmpeg
        try:
            subprocess.run(['ffmpeg', '-version'], capture_output=True, timeout=5)
            print("✅ FFmpeg可用")
            self.ffmpeg_available = True
        except:
            print("⚠️  FFmpeg未安装，将使用HTTP测速")
            self.ffmpeg_available = False
            
        return True

    def validate_url(self, url):
        """验证URL格式"""
        try:
            result = urlparse(url)
            return all([result.scheme in ['http', 'https'], result.netloc])
        except:
            return False

    def fetch_streams_from_url(self, url):
        """从URL获取流数据"""
        print(f"📡 正在爬取源: {url}")
        try:
            response = self.session.get(url, timeout=self.REQUEST_CONFIG['timeout'])
            response.encoding = 'utf-8'
            if response.status_code == 200:
                content_length = len(response.text)
                print(f"✅ 成功获取数据: {url} ({content_length} 字符)")
                return response.text
            else:
                print(f"❌ 获取数据失败，状态码: {response.status_code} - {url}")
        except Exception as e:
            print(f"❌ 请求错误: {e} - {url}")
        return None

    def fetch_all_streams(self):
        """获取所有源的流数据"""
        print("🚀 开始智能多源抓取...")
        all_streams = []
        successful_sources = 0
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(self.fetch_streams_from_url, url): url for url in self.SOURCE_URLS}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    if content := future.result():
                        all_streams.append(content)
                        successful_sources += 1
                except Exception as e:
                    print(f"❌ 处理 {url} 时发生错误: {e}")
        
        print(f"✅ 成功获取 {successful_sources}/{len(self.SOURCE_URLS)} 个源的数据")
        return "\n".join(all_streams)

    def parse_m3u(self, content):
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

    def parse_txt(self, content):
        """解析TXT格式"""
        streams = []
        
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '#genre#' in line:
                continue
            
            # 多种分隔符支持：逗号、空格、制表符等
            if ',' in line:
                parts = line.split(',', 1)
                if len(parts) == 2:
                    program_name = parts[0].strip()
                    # 从第二部分提取URL
                    url_match = self.url_pattern.search(parts[1])
                    if url_match:
                        stream_url = url_match.group()
                        if self.validate_url(stream_url):
                            streams.append({
                                "program_name": program_name,
                                "stream_url": stream_url,
                                "group": "默认分组"
                            })
            else:
                # 尝试从行中提取URL
                url_match = self.url_pattern.search(line)
                if url_match:
                    stream_url = url_match.group()
                    program_name = line.replace(stream_url, '').strip()
                    if program_name and self.validate_url(stream_url):
                        streams.append({
                            "program_name": program_name,
                            "stream_url": stream_url,
                            "group": "默认分组"
                        })
        
        return streams

    def organize_streams(self, content):
        """整理流数据"""
        if not content:
            print("❌ 没有内容可处理")
            return pd.DataFrame()
            
        print("🔍 解析流数据...")
        
        # 自动检测格式并解析
        if content.startswith("#EXTM3U"):
            streams = self.parse_m3u(content)
        else:
            streams = self.parse_txt(content)
        
        if not streams:
            print("❌ 未能解析出任何流数据")
            return pd.DataFrame()
            
        df = pd.DataFrame(streams)
        
        # 数据清理
        initial_count = len(df)
        df = df.dropna()
        df = df[df['program_name'].str.len() > 0]
        df = df[df['stream_url'].str.startswith(('http://', 'https://'))]
        
        # 去重
        df = df.drop_duplicates(subset=['program_name', 'stream_url'])
        
        print(f"📊 数据清理: {initial_count} -> {len(df)} 个流")
        return df

    def load_template(self):
        """加载频道模板"""
        template_file = self.FILE_CONFIG['template_file']
        if not os.path.exists(template_file):
            print(f"❌ 模板文件 {template_file} 不存在")
            return None
            
        print(f"📋 加载模板文件: {template_file}")
        categories = {}
        current_category = None
        
        try:
            with open(template_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                        
                    # 检测分类行
                    if match := self.category_pattern.match(line):
                        current_category = match.group(1).strip()
                        categories[current_category] = []
                    elif current_category and line and not line.startswith('#'):
                        # 频道行
                        if ',' in line:
                            channel_name = line.split(',')[0].strip()
                        else:
                            channel_name = line.strip()
                        if channel_name:
                            categories[current_category].append(channel_name)
        except Exception as e:
            print(f"❌ 读取模板文件失败: {e}")
            return None
        
        if not categories:
            print("❌ 模板文件中未找到有效的频道分类")
            return None
            
        print(f"📁 模板分类: {list(categories.keys())}")
        total_channels = sum(len(channels) for channels in categories.values())
        print(f"📺 模板频道总数: {total_channels}")
        
        return categories

    def similarity_score(self, str1, str2):
        """计算两个字符串的相似度分数"""
        if not str1 or not str2:
            return 0
            
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
        
        # 共同字符比例
        common_chars = len(set(str1_clean) & set(str2_clean))
        total_chars = len(set(str1_clean) | set(str2_clean))
        
        if total_chars > 0:
            similarity = (common_chars / total_chars) * 80
            return int(similarity)
        
        return 0

    def speed_test_ffmpeg(self, stream_url):
        """使用FFmpeg进行流媒体测速"""
        if not self.ffmpeg_available:
            return False, float('inf')
            
        temp_file = os.path.join(self.FILE_CONFIG['temp_dir'], f'test_{abs(hash(stream_url))}.ts')
        
        try:
            # 使用FFmpeg测试流媒体可访问性
            cmd = [
                'ffmpeg',
                '-y',  # 覆盖输出文件
                '-timeout', '3000000',  # 3秒超时（微秒）
                '-i', stream_url,
                '-t', '2',  # 只测试2秒
                '-c', 'copy',
                '-f', 'mpegts',
                '-max_muxing_queue_size', '1024',
                temp_file
            ]
            
            start_time = time.time()
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=5  # 总超时时间
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

    def speed_test_simple(self, stream_url):
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

    def filter_and_sort_sources(self, sources_df, template_channels):
        """根据模板过滤和排序源"""
        print("🎯 开始频道匹配和源筛选...")
        
        # 创建频道映射（模糊匹配）
        channel_mapping = {}
        match_results = []
        
        for template_channel in template_channels:
            best_match = None
            best_score = 0
            best_source_channel = None
            
            # 在源数据中寻找最佳匹配
            for source_channel in sources_df['program_name'].unique():
                score = self.similarity_score(template_channel, source_channel)
                if score > best_score and score > 50:  # 相似度阈值提高到50
                    best_score = score
                    best_match = template_channel
                    best_source_channel = source_channel
            
            if best_match and best_source_channel:
                channel_mapping[best_source_channel] = best_match
                match_results.append((best_match, best_source_channel, best_score))
        
        # 打印匹配结果
        for template_channel, source_channel, score in sorted(match_results, key=lambda x: x[2], reverse=True)[:10]:
            print(f"  ✅ 匹配: {template_channel} <- {source_channel} (分数: {score})")
        
        if len(match_results) > 10:
            print(f"  ... 还有 {len(match_results) - 10} 个匹配")
        
        # 过滤数据，只保留匹配的频道
        matched_mask = sources_df['program_name'].isin(channel_mapping.keys())
        filtered_df = sources_df[matched_mask].copy()
        
        # 将源频道名称映射回模板频道名称
        filtered_df['program_name'] = filtered_df['program_name'].map(channel_mapping)
        
        print(f"✅ 频道匹配完成: {len(filtered_df)} 个流匹配到 {len(set(channel_mapping.values()))} 个模板频道")
        return filtered_df

    def speed_test_sources(self, sources_df):
        """对源进行测速"""
        print("⏱️  开始智能测速...")
        
        if sources_df.empty:
            print("❌ 没有需要测速的源")
            return pd.DataFrame()
            
        results = []
        total_sources = len(sources_df)
        self.total_count = total_sources
        self.processed_count = 0
        
        def test_single_source(row):
            program_name = row['program_name']
            stream_url = row['stream_url']
            
            self.processed_count += 1
            current = self.processed_count
            total = self.total_count
            
            print(f"  🔍 测试 {current}/{total}: {program_name[:25]:<25}...", end=' ')
            
            # 根据URL类型选择测速方法
            if any(ext in stream_url.lower() for ext in ['.m3u8', '.ts', '.flv', '.mp4']):
                # 流媒体格式，优先使用FFmpeg
                if self.ffmpeg_available:
                    accessible, speed = self.speed_test_ffmpeg(stream_url)
                else:
                    accessible, speed = self.speed_test_simple(stream_url)
            else:
                # 其他格式使用简单测速
                accessible, speed = self.speed_test_simple(stream_url)
            
            if accessible:
                print(f"✅ ({(speed):.2f}s)")
            else:
                print("❌")
            
            return {
                'program_name': program_name,
                'stream_url': stream_url,
                'accessible': accessible,
                'speed': speed
            }
        
        # 使用线程池进行并发测速（限制并发数）
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(test_single_source, row) for _, row in sources_df.iterrows()]
            
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=10)
                    results.append(result)
                except Exception as e:
                    print(f"    ❌ 测速异常: {e}")
        
        # 转换为DataFrame
        result_df = pd.DataFrame(results)
        
        # 过滤不可访问的源
        accessible_df = result_df[result_df['accessible']].copy()
        
        print(f"📊 测速完成: {len(accessible_df)}/{total_sources} 个源可用")
        
        return accessible_df

    def generate_final_data(self, speed_tested_df, template_categories):
        """生成最终数据"""
        print("🎨 生成最终文件...")
        
        final_data = {}
        total_sources = 0
        
        for category, channels in template_categories.items():
            final_data[category] = {}
            
            for channel in channels:
                # 获取该频道的所有源
                channel_sources = speed_tested_df[speed_tested_df['program_name'] == channel]
                
                if not channel_sources.empty:
                    # 按速度排序并取前N个
                    sorted_sources = channel_sources.sort_values('speed').head(
                        self.CHANNEL_CONFIG['max_sources_per_channel']
                    )
                    final_data[category][channel] = sorted_sources[['stream_url', 'speed']].to_dict('records')
                    source_count = len(sorted_sources)
                    total_sources += source_count
                    print(f"  ✅ {category}-{channel}: {source_count}个源")
                else:
                    final_data[category][channel] = []
                    print(f"  ❌ {category}-{channel}: 无可用源")
        
        print(f"📦 总共收集到 {total_sources} 个有效源")
        return final_data

    def save_output_files(self, final_data):
        """保存输出文件"""
        print("💾 保存文件...")
        
        # 保存TXT格式
        try:
            with open(self.FILE_CONFIG['output_txt'], 'w', encoding='utf-8') as f:
                for category, channels in final_data.items():
                    f.write(f"{category},#genre#\n")
                    
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f"{channel},{source['stream_url']}\n")
                    
                    f.write("\n")
            print(f"✅ TXT文件已保存: {os.path.abspath(self.FILE_CONFIG['output_txt'])}")
        except Exception as e:
            print(f"❌ 保存TXT文件失败: {e}")
            return False
        
        # 保存M3U格式
        try:
            with open(self.FILE_CONFIG['output_m3u'], 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                
                for category, channels in final_data.items():
                    for channel, sources in channels.items():
                        for source in sources:
                            f.write(f'#EXTINF:-1 tvg-name="{channel}" group-title="{category}",{channel}\n')
                            f.write(f"{source['stream_url']}\n")
            print(f"✅ M3U文件已保存: {os.path.abspath(self.FILE_CONFIG['output_m3u'])}")
        except Exception as e:
            print(f"❌ 保存M3U文件失败: {e}")
            return False
            
        return True

    def print_statistics(self, final_data):
        """打印统计信息"""
        print("\n" + "="*50)
        print("📈 生成统计报告")
        print("="*50)
        
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
            if len(no_source_channels) <= 15:
                for channel in no_source_channels:
                    print(f"    ❌ {channel}")

    def cleanup(self):
        """清理临时文件"""
        try:
            temp_dir = self.FILE_CONFIG['temp_dir']
            if os.path.exists(temp_dir):
                for file in os.listdir(temp_dir):
                    file_path = os.path.join(temp_dir, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                print("✅ 临时文件清理完成")
        except Exception as e:
            print(f"⚠️  清理临时文件时出错: {e}")

    def create_demo_template(self):
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

地方频道,#genre#
北京新闻
上海新闻
广州综合
深圳卫视
"""
        try:
            with open(self.FILE_CONFIG['template_file'], 'w', encoding='utf-8') as f:
                f.write(demo_content)
            print(f"✅ 已创建示例模板文件: {self.FILE_CONFIG['template_file']}")
            print("💡 请编辑此文件，添加您需要的频道列表")
            return True
        except Exception as e:
            print(f"❌ 创建模板文件失败: {e}")
            return False

    def run(self):
        """主运行函数"""
        print("=" * 60)
        print("🎬 IPTV智能管理工具 - 完整版 v1.0")
        print("=" * 60)
        
        # 检查依赖
        if not self.check_dependencies():
            print("❌ 依赖检查失败，程序退出")
            return
        
        # 检查模板文件，如果不存在则创建示例
        if not os.path.exists(self.FILE_CONFIG['template_file']):
            print("📝 未找到模板文件，创建示例模板...")
            if not self.create_demo_template():
                return
            print("请编辑 demo.txt 文件，添加您需要的频道，然后重新运行程序")
            return
        
        start_time = time.time()
        
        try:
            # 1. 加载模板
            print("\n📋 步骤 1/7: 加载频道模板")
            template_categories = self.load_template()
            if not template_categories:
                return
            
            # 2. 获取所有源数据
            print("\n🌐 步骤 2/7: 获取源数据")
            content = self.fetch_all_streams()
            if not content:
                print("❌ 未能获取任何源数据")
                return
            
            # 3. 整理源数据
            print("\n🔧 步骤 3/7: 整理源数据")
            sources_df = self.organize_streams(content)
            if sources_df.empty:
                print("❌ 未能解析出有效的流数据")
                return
            
            # 4. 获取所有模板频道
            all_template_channels = []
            for channels in template_categories.values():
                all_template_channels.extend(channels)
            
            # 5. 过滤和匹配频道
            print("\n🎯 步骤 4/7: 频道匹配")
            filtered_df = self.filter_and_sort_sources(sources_df, all_template_channels)
            if filtered_df.empty:
                print("❌ 没有匹配到任何模板频道")
                return
            
            # 6. 测速
            print("\n⚡ 步骤 5/7: 源测速")
            speed_tested_df = self.speed_test_sources(filtered_df)
            if speed_tested_df.empty:
                print("❌ 没有可用的源通过测速")
                return
            
            # 7. 生成最终数据
            print("\n🎨 步骤 6/7: 生成播放列表")
            final_data = self.generate_final_data(speed_tested_df, template_categories)
            
            # 8. 保存文件
            print("\n💾 步骤 7/7: 保存文件")
            if not self.save_output_files(final_data):
                print("❌ 文件保存失败")
                return
            
            # 9. 打印统计
            self.print_statistics(final_data)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            print("\n🎉 处理完成!")
            print(f"⏰ 总耗时: {elapsed_time:.2f} 秒")
            print(f"📁 生成文件位置:")
            print(f"   📄 {os.path.abspath(self.FILE_CONFIG['output_txt'])}")
            print(f"   📄 {os.path.abspath(self.FILE_CONFIG['output_m3u'])}")
            
        except KeyboardInterrupt:
            print("\n⚠️  用户中断操作")
        except Exception as e:
            print(f"\n❌ 程序运行出错: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理临时文件
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
