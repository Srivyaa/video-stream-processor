#!/usr/bin/env python3
"""
Video Link Processor for GitHub Actions
Reads video links from links.txt and generates streamable URLs in JSON format
"""

import json
import uuid
import datetime
import re
import os
import sys
import time
import random
from urllib.parse import urlparse
import yt_dlp

class VideoLinkProcessor:
    def __init__(self):
        self.output_data = []
        self.processed_count = 0
        self.failed_count = 0
        
    def generate_uuids(self):
        """Generate all required UUIDs for a station"""
        return {
            "changeuuid": str(uuid.uuid4()),
            "stationuuid": str(uuid.uuid4()),
            "serveruuid": str(uuid.uuid4())
        }
    
    def get_current_timestamp(self):
        """Get current timestamp in required format - FIXED datetime warning"""
        current_time = datetime.datetime.now(datetime.UTC)
        iso_time = current_time.isoformat().replace('+00:00', 'Z')
        formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        return formatted_time, iso_time
    
    def extract_video_info(self, url):
        """Extract video information using yt-dlp with better error handling"""
        max_retries = 2
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                # Enhanced yt-dlp options to avoid bot detection
                ydl_opts = {
                    'quiet': True,
                    'no_warnings': False,  # Set to False to see warnings
                    'extract_flat': False,
                    'socket_timeout': 60,
                    'extractaudio': True,
                    'audioformat': 'best',
                    'noplaylist': True,
                    'no_check_certificate': True,
                    'ignoreerrors': True,
                    'no_part': True,
                    'ratelimit': 1024000,  # Limit download speed
                    'throttled_rate': 1024000,
                    'extractor_args': {
                        'youtube': {
                            'player_client': ['android', 'web'],
                            'player_skip': ['configs', 'webpage'],
                        }
                    },
                    'http_headers': {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-us,en;q=0.5',
                        'Accept-Encoding': 'gzip,deflate',
                        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
                        'Connection': 'keep-alive',
                    }
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    
                    if not info:
                        print(f"   ⚠️ No info extracted for {url}")
                        return None
                    
                    # Get the best audio stream URL
                    if 'url' in info:
                        stream_url = info['url']
                    else:
                        # Fallback to format selection
                        formats = info.get('formats', [])
                        audio_formats = [f for f in formats if f.get('vcodec') == 'none' and f.get('acodec') != 'none']
                        if audio_formats:
                            # Prefer formats with known audio codecs
                            preferred_formats = [f for f in audio_formats if any(codec in f.get('acodec', '') for codec in ['mp4a', 'aac', 'mp3'])]
                            if preferred_formats:
                                stream_url = preferred_formats[-1]['url']
                            else:
                                stream_url = audio_formats[-1]['url']
                        else:
                            # Last resort - take any format
                            stream_url = formats[-1]['url'] if formats else url
                
                return {
                    'title': info.get('title', 'Unknown Title'),
                    'description': info.get('description', ''),
                    'thumbnail': info.get('thumbnail', ''),
                    'stream_url': stream_url,
                    'duration': info.get('duration', 0),
                    'uploader': info.get('uploader', ''),
                    'view_count': info.get('view_count', 0)
                }
                
            except Exception as e:
                error_msg = str(e)
                print(f"   ⚠️ Attempt {attempt + 1} failed for {url}: {error_msg}")
                
                # Handle specific YouTube errors
                if "Sign in to confirm you're not a bot" in error_msg:
                    print(f"   🔒 YouTube bot detection triggered for {url}")
                    # Try alternative extraction method
                    return self.extract_with_fallback(url)
                elif "Video unavailable" in error_msg:
                    print(f"   ❌ Video unavailable: {url}")
                    return None
                elif "Private video" in error_msg:
                    print(f"   🔒 Private video: {url}")
                    return None
                elif "This video is not available" in error_msg:
                    print(f"   ❌ Video not available: {url}")
                    return None
                
                if attempt < max_retries - 1:
                    sleep_time = retry_delay * (attempt + 1)
                    print(f"   💤 Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    print(f"   ❌ All attempts failed for {url}")
                    return None
    
    def extract_with_fallback(self, url):
        """Alternative extraction method for problematic videos"""
        try:
            # Try a simpler approach with different parameters
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,  # Try flat extraction first
                'force_json': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                if info and 'url' not in info:
                    # If flat extraction worked but no URL, try full extraction with different client
                    ydl_opts_full = {
                        'quiet': True,
                        'no_warnings': True,
                        'extract_flat': False,
                        'extractor_args': {'youtube': {'player_client': ['ios']}},
                    }
                    
                    with yt_dlp.YoutubeDL(ydl_opts_full) as ydl_full:
                        info_full = ydl_full.extract_info(url, download=False)
                        if info_full and 'url' in info_full:
                            info = info_full
            
            if info:
                return {
                    'title': info.get('title', 'Unknown Title'),
                    'description': info.get('description', ''),
                    'thumbnail': info.get('thumbnail', ''),
                    'stream_url': info.get('url', url),  # Fallback to original URL
                    'duration': info.get('duration', 0),
                    'uploader': info.get('uploader', ''),
                    'view_count': info.get('view_count', 0)
                }
            
        except Exception as e:
            print(f"   ⚠️ Fallback extraction also failed: {str(e)}")
        
        return None
    
    def is_hls_url(self, url):
        """Check if URL is HLS stream"""
        return '.m3u8' in url.lower()
    
    def get_file_extension(self, url):
        """Extract file extension from URL"""
        parsed = urlparse(url)
        path = parsed.path
        if '.' in path:
            return path.split('.')[-1].split('?')[0].lower()
        return ''
    
    def get_codec_info(self, url):
        """Determine codec and bitrate from URL"""
        ext = self.get_file_extension(url)
        codec_map = {
            'm4a': 'MP4A',
            'mp4': 'MP4A',
            'mp3': 'MP3',
            'aac': 'AAC',
            'webm': 'OPUS',
            'ogg': 'OGG',
            'm3u8': 'HLS'
        }
        
        bitrate_map = {
            'm4a': 128,
            'mp4': 128,
            'mp3': 128,
            'aac': 128,
            'webm': 128,
            'ogg': 128,
            'm3u8': 128
        }
        
        return codec_map.get(ext, 'MP4A'), bitrate_map.get(ext, 128)
    
    def extract_tags_from_title(self, title):
        """Extract potential tags from video title"""
        words = re.findall(r'\b\w+\b', title.lower())
        common_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'official', 'video', 'audio', 'hd', 'tamil', 'song'}
        tags = [word for word in words if word not in common_words and len(word) > 2]
        return ",".join(tags[:6])  # Reduced number of tags
    
    def guess_language_from_title(self, title):
        """Guess language from video title"""
        title_lower = title.lower()
        
        language_indicators = {
            'tamil': {'tamil', 'tamizh', 'tam'},
            'hindi': {'hindi', 'hind'},
            'english': {'english', 'eng'},
            'telugu': {'telugu', 'tel'},
            'malayalam': {'malayalam', 'mal'},
            'kannada': {'kannada', 'kan'},
        }
        
        for language, indicators in language_indicators.items():
            if any(indicator in title_lower for indicator in indicators):
                return language.capitalize(), language.upper()[:5]
        
        # Default to Tamil for nursery rhymes
        return "Tamil", "TAMIL"
    
    def create_filename(self, title, url):
        """Create filename from title"""
        safe_title = re.sub(r'[^\w\s-]', '', title)
        safe_title = re.sub(r'[-\s]+', '_', safe_title)
        ext = self.get_file_extension(url)
        return f"{safe_title}.{ext}" if ext else f"{safe_title}.m4a"
    
    def process_video_link(self, url, index):
        """Process a single video link and return station data"""
        print(f"🔗 Processing {index + 1}: {url}")
        
        uuids = self.generate_uuids()
        last_change_time, last_change_time_iso = self.get_current_timestamp()
        
        video_info = self.extract_video_info(url)
        if not video_info:
            self.failed_count += 1
            print(f"   ❌ Failed to extract info from: {url}")
            return None
        
        title = video_info['title']
        stream_url = video_info['stream_url']
        
        # Check if we got a valid stream URL (not the original YouTube URL)
        if 'youtube.com/watch' in stream_url:
            print(f"   ⚠️ Could not extract direct stream URL for: {title}")
            self.failed_count += 1
            return None
        
        is_hls = 1 if self.is_hls_url(stream_url) else 0
        codec, bitrate = self.get_codec_info(stream_url)
        tags = self.extract_tags_from_title(title)
        language, language_code = self.guess_language_from_title(title)
        filename = self.create_filename(title, stream_url)
        
        station_data = {
            "changeuuid": uuids["changeuuid"],
            "stationuuid": uuids["stationuuid"],
            "serveruuid": uuids["serveruuid"],
            "name": title[:80],
            "url": url,
            "url_resolved": stream_url,
            "homepage": "https://youtube.com",
            "favicon": video_info.get('thumbnail', 'https://youtube.com/favicon.ico'),
            "tags": tags[:80],
            "country": f"User Defined ({language} Videos)",
            "countrycode": language_code,
            "state": f"{language} State",
            "language": language,
            "languagecodes": language_code.lower()[:2],
            "votes": 0,
            "lastchangetime": last_change_time,
            "lastchangetime_iso8601": last_change_time_iso,
            "codec": codec,
            "bitrate": bitrate,
            "file_name_from_url": filename[:80],
            "hls": is_hls,
            "lastcheckok": 1,
            "lastchecktime": last_change_time,
            "lastchecktime_iso8601": last_change_time_iso,
            "lastcheckoktime": last_change_time,
            "lastcheckoktime_iso8601": last_change_time_iso,
            "lastlocalchecktime": last_change_time,
            "lastlocalchecktime_iso8601": last_change_time_iso,
            "clicktimestamp": last_change_time,
            "clicktimestamp_iso8601": last_change_time_iso,
            "clickcount": 0,
            "clicktrend": 0,
            "ssl_error": 0,
            "geo_lat": None,
            "geo_long": None,
            "geo_distance": None,
            "has_extended_info": False
        }
        
        self.processed_count += 1
        print(f"   ✅ Successfully processed: {title[:50]}...")
        return station_data
    
    def read_links_from_file(self, filename="links.txt"):
        """Read video links from text file"""
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                links = [line.strip() for line in file if line.strip() and not line.startswith('#')]
            return list(set(links))  # Remove duplicates
        except FileNotFoundError:
            print(f"❌ Error: {filename} not found!")
            return []
        except Exception as e:
            print(f"❌ Error reading {filename}: {str(e)}")
            return []
    
    def save_to_json(self, data, filename="output.json"):
        """Save data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=2, ensure_ascii=False)
            print(f"✅ Successfully saved {len(data)} stations to {filename}")
            
            # Verify the file was written
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                return True
            else:
                print(f"❌ File {filename} was created but is empty or doesn't exist")
                return False
                
        except Exception as e:
            print(f"❌ Error saving to {filename}: {str(e)}")
            return False
    
    def process_all_links(self):
        """Main method to process all links"""
        links = self.read_links_from_file()
        
        if not links:
            print("❌ No links found to process!")
            return False
        
        print(f"📋 Found {len(links)} unique links to process...")
        
        for index, link in enumerate(links):
            try:
                station_data = self.process_video_link(link, index)
                if station_data:
                    self.output_data.append(station_data)
                else:
                    print(f"   ❌ Failed to process: {link}")
            except Exception as e:
                print(f"   ❌ Unexpected error processing {link}: {str(e)}")
                self.failed_count += 1
            
            # Randomized rate limiting to avoid detection
            if index < len(links) - 1:
                sleep_time = random.uniform(3, 8)  # Random delay between 3-8 seconds
                print(f"   💤 Waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
        
        if self.output_data:
            success = self.save_to_json(self.output_data)
            print(f"\n📊 Processing complete!")
            print(f"✅ Successfully processed: {self.processed_count}")
            print(f"❌ Failed: {self.failed_count}")
            print(f"📊 Total unique links: {len(links)}")
            return success
        else:
            print("❌ No data was processed successfully.")
            return False

def main():
    """Main function"""
    print("🚀 Starting Video Link Processor...")
    print(f"📅 Current time: {datetime.datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z')}")
    
    # Check if running in GitHub Actions
    if os.getenv('GITHUB_ACTIONS') == 'true':
        print("🏃 Running in GitHub Actions environment")
    
    # Check if links.txt exists
    if not os.path.exists("links.txt"):
        print("❌ links.txt not found. Please create it with your video URLs.")
        sys.exit(1)
    
    processor = VideoLinkProcessor()
    success = processor.process_all_links()
    
    if success and processor.processed_count > 0:
        print("✅ Script completed successfully!")
        sys.exit(0)
    else:
        print("❌ Script completed with errors or no videos processed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
