/**
 * SeekStreaming Video Downloader - Auto Mode
 * 
 * Automatically:
 * 1. Load https://seekstream.embedseek.com/#{video_id}
 * 2. Capture HLS URL from network requests
 * 3. Download with yt-dlp
 * 
 * Usage:
 *   node test_seekstreaming_download.js
 *   VIDEO_ID=video_id node test_seekstreaming_download.js
 */

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer');

const OUTPUT_DIR = path.join(__dirname, '../downloads');
const VIDEO_ID = process.env.VIDEO_ID || process.env.TEST_VIDEO_ID || 'ugesu';

function ensureOutputDir() {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }
}

async function downloadWithYtdlp(streamUrl, videoId) {
  const outputPath = path.join(OUTPUT_DIR, `seekstream_${videoId}.mp4`);
  
  return new Promise((resolve, reject) => {
    console.log(`\n📥 Downloading...`);
    
    const proc = spawn('yt-dlp', [
      '-f', 'best',
      '-o', outputPath,
      '--add-header', 'User-Agent:Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      '--add-header', 'Referer:https://seekstream.embedseek.com/',
      streamUrl
    ], { stdio: 'inherit' });
    
    proc.on('close', (code) => {
      if (code === 0 && fs.existsSync(outputPath)) {
        const size = fs.statSync(outputPath).size;
        console.log(`\n✅ Done! ${(size / 1024 / 1024).toFixed(2)} MB`);
        resolve(outputPath);
      } else {
        reject(new Error(`Download failed with code ${code}`));
      }
    });
    
    proc.on('error', reject);
  });
}

async function autoDownload(videoId) {
  const url = `https://seekstream.embedseek.com/#${videoId}`;
  
  console.log(`Loading: ${url}`);
  
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await page.setViewport({ width: 1280, height: 720 });
    
    const allRequests = new Set();
    
    // Enable CDP for Network domain
    const client = await page.target().createCDPSession();
    await client.send('Network.enable');
    
    client.on('Network.requestWillBeSent', (params) => {
      const reqUrl = params.request.url;
      allRequests.add(reqUrl);
      
      // Check for stream domain
      if (reqUrl.includes('emergingtechhub') || reqUrl.includes('.txt') || reqUrl.includes('.m3u8') || reqUrl.includes('k5')) {
        console.log(`  📡 STREAM: ${reqUrl.substring(0, 80)}...`);
      }
    });
    
    console.log(`Loading page...`);
    await page.goto(url, { waitUntil: 'networkidle0', timeout: 60000 });
    
    console.log(`Waiting for player...`);
    await new Promise(r => setTimeout(r, 5000));
    
    // Try to trigger video playback
    console.log(`Clicking to play...`);
    await page.mouse.click(640, 360);
    await new Promise(r => setTimeout(r, 3000));
    
    // Call play() on video
    await page.evaluate(() => {
      const videos = document.querySelectorAll('video');
      videos.forEach(v => v.play().catch(() => {}));
    });
    
    // Wait more for stream
    console.log(`Waiting for stream...`);
    await new Promise(r => setTimeout(r, 10000));
    
    await browser.close();
    
    // Look for stream URL in all requests
    const streamUrl = [...allRequests].find(u => 
      u.includes('emergingtechhub') || 
      u.includes('/v4/') ||
      u.includes('.txt') ||
      u.includes('.m3u8')
    );
    
    if (streamUrl) {
      console.log(`\n🎬 Found: ${streamUrl}`);
      await downloadWithYtdlp(streamUrl, videoId);
    } else {
      // Fallback to known pattern
      const fallbackUrl = `https://suo.emergingtechhubonline.store/v4/k5/${videoId}/index-f1-v1-a1.txt`;
      console.log(`\n🔄 Using fallback: ${fallbackUrl}`);
      await downloadWithYtdlp(fallbackUrl, videoId);
    }
    
  } catch (error) {
    if (browser) await browser.close().catch(() => {});
    throw error;
  }
}

async function main() {
  ensureOutputDir();
  
  console.log('========================================');
  console.log('  SeekStreaming Auto Downloader');
  console.log('========================================');
  console.log(`\nVideo ID: ${VIDEO_ID}`);
  
  try {
    await autoDownload(VIDEO_ID);
  } catch (error) {
    console.error(`\n❌ Error: ${error.message}`);
    process.exit(1);
  }
}

main();