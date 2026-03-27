/**
 * Test Script: Extract video URL from SeekStreaming
 */

const axios = require('axios');
const fs = require('fs');
const path = require('path');

const API_KEY = process.env.SEEK_API_KEY || 'your_api_key_here';
const BASE_URL = process.env.SEEK_API_BASE_URL || 'https://seekstreaming.com';

// Test video IDs
const TEST_VIDEO_IDS = [
  // Replace with real video IDs from your account
  'your_video_id_1',
  'your_video_id_2'
];

async function testApiGetVideo(videoId) {
  console.log(`\n🔍 Testing SeekStreaming API /video/manage/${videoId}`);
  
  try {
    const response = await axios.get(`${BASE_URL}/api/v1/video/manage/${videoId}`, {
      headers: {
        'api-token': API_KEY,
        'Accept': 'application/json'
      },
      timeout: 15000
    });
    
    console.log(`  ✅ API Response:`);
    console.log(JSON.stringify(response.data, null, 2));
    
    // Check for download_url
    const data = response.data.data || response.data;
    if (data.download_url) {
      console.log(`\n  🎯 Found download_url: ${data.download_url}`);
      return data.download_url;
    } else {
      console.log(`\n  ⚠️  No download_url in response`);
      console.log(`  Available keys: ${Object.keys(data).join(', ')}`);
    }
    
    return null;
    
  } catch (error) {
    console.log(`  ❌ API Error: ${error.message}`);
    if (error.response) {
      console.log(`     Status: ${error.response.status}`);
      console.log(`     Data: ${JSON.stringify(error.response.data)}`);
    }
    return null;
  }
}

async function testListVideos() {
  console.log(`\n📋 Testing SeekStreaming API /video/manage (list)`);
  
  try {
    const response = await axios.get(`${BASE_URL}/api/v1/video/manage`, {
      headers: {
        'api-token': API_KEY,
        'Accept': 'application/json'
      },
      params: { page: 1, perPage: 10 },
      timeout: 15000
    });
    
    console.log(`  ✅ List Response:`);
    const videos = response.data.data || response.data;
    
    if (Array.isArray(videos) && videos.length > 0) {
      console.log(`  Found ${videos.length} videos`);
      
      // Show first video structure
      console.log(`\n  📝 First video keys: ${Object.keys(videos[0]).join(', ')}`);
      console.log(`  First video: ${JSON.stringify(videos[0], null, 2)}`);
      
      return videos[0];
    } else {
      console.log(`  No videos found or unexpected response`);
    }
    
    return null;
    
  } catch (error) {
    console.log(`  ❌ List Error: ${error.message}`);
    return null;
  }
}

async function testEmbedPage(videoId) {
  console.log(`\n🌐 Testing SeekStreaming embed page for: ${videoId}`);
  
  const embedUrl = `https://seekstream.embedseek.com/#${videoId}`;
  console.log(`  URL: ${embedUrl}`);
  
  try {
    const response = await axios.get(embedUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
      },
      timeout: 15000,
      maxRedirects: 5
    });
    
    const html = response.data;
    console.log(`  ✅ Page loaded (${html.length} bytes)`);
    
    // Look for video URLs or player configs
    const patterns = [
      // Direct video URL
      /https:\/\/[^\s"'<>]+\.(mp4|m3u8|webm|mkv)(\?[^"'<>]*)?/gi,
      
      // Player config
      /sources\s*:\s*\[\s*\{[^}]*\}/i,
      
      // data-src
      /data-src=["']([^"']+)["']/i,
      
      // video src
      /<video[^>]+src=["']([^"']+)["']/i,
      
      // HLS playlist
      /https:\/\/[^\s"'<>]+\.m3u8[^\s"'<>]*/gi,
    ];
    
    for (const pattern of patterns) {
      const matches = html.match(pattern);
      if (matches) {
        console.log(`  ✅ Pattern matched! Found URLs:`);
        const unique = [...new Set(matches)];
        unique.forEach(url => console.log(`     - ${url.substring(0, 100)}...`));
        return unique[0];
      }
    }
    
    console.log(`  ⚠️  No video patterns found in HTML`);
    
    // Save for debugging
    const debugFile = path.join(__dirname, `debug_seek_${videoId}_${Date.now()}.html`);
    fs.writeFileSync(debugFile, html);
    console.log(`     Saved to: ${debugFile}`);
    
    return null;
    
  } catch (error) {
    console.log(`  ❌ Page Error: ${error.message}`);
    return null;
  }
}

async function testPlayerUrl(videoId) {
  console.log(`\n🎬 Testing player URL format: /v/${videoId}`);
  
  const playerUrl = `https://seekstreaming.com/v/${videoId}`;
  
  try {
    const response = await axios.head(playerUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      },
      timeout: 10000,
      maxRedirects: 5
    });
    
    console.log(`  Status: ${response.status}`);
    console.log(`  Final URL: ${response.request?.res?.responseUrl || playerUrl}`);
    
    // Check headers for streaming info
    const contentType = response.headers['content-type'];
    const contentLength = response.headers['content-length'];
    console.log(`  Content-Type: ${contentType}`);
    console.log(`  Content-Length: ${contentLength}`);
    
    return response.request?.res?.responseUrl;
    
  } catch (error) {
    console.log(`  ❌ Error: ${error.message}`);
    return null;
  }
}

async function main() {
  console.log('========================================');
  console.log('  SeekStreaming Video URL Extraction Test');
  console.log('========================================');
  console.log(`\nAPI Base URL: ${BASE_URL}`);
  console.log(`API Key: ${API_KEY === 'your_api_key_here' ? '(not set)' : '***'}`);
  
  // Test list videos first (no video ID needed)
  const firstVideo = await testListVideos();
  
  // If we have API key and found videos, test get video
  if (API_KEY !== 'your_api_key_here') {
    if (firstVideo?.id) {
      await testApiGetVideo(firstVideo.id);
    }
    
    // Test configured video IDs
    for (const videoId of TEST_VIDEO_IDS) {
      if (videoId !== 'your_video_id_1') {
        await testApiGetVideo(videoId);
      }
    }
  } else {
    console.log('\n⚠️  Set SEEK_API_KEY env var to test API');
  }
  
  // Test embed page
  for (const videoId of TEST_VIDEO_IDS) {
    if (videoId !== 'your_video_id_1') {
      await testEmbedPage(videoId);
      await testPlayerUrl(videoId);
    }
  }
  
  console.log('\n========================================');
  console.log('  Test Complete');
  console.log('========================================');
}

main().catch(console.error);
