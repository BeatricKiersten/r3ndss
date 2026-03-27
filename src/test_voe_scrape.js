/**
 * Test Script: Extract video URL from Voe.sx embed page
 * 
 * This tests if we can scrape direct video URLs from Voe.sx
 * for use in reupload mechanism.
 */

const axios = require('axios');
const fs = require('fs');
const path = require('path');

const API_KEY = process.env.VOE_API_KEY || 'your_api_key_here';

// Test file codes
const TEST_FILE_CODES = [
  // Replace with real file codes from your account
  'your_file_code_1',
  'your_file_code_2'
];

async function extractVoeVideoUrl(fileCode) {
  console.log(`\n🔍 Testing Voe.sx file: ${fileCode}`);
  
  const embedUrl = `https://voe.sx/e/${fileCode}`;
  
  try {
    // Fetch embed page with proper headers
    const response = await axios.get(embedUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
      },
      timeout: 30000
    });

    const html = response.data;
    
    // Try multiple extraction patterns
    const patterns = [
      // Pattern 1: <source src="...">
      /<source\s+src=["']([^"']+)["']/i,
      
      // Pattern 2: eval(...) with sources array
      /eval\s*\(\s*function\s*\([^)]*\)\s*\{[^}]*sources\s*:\s*\[\s*\{[^}]*file\s*:\s*["']([^"']+)["']/i,
      
      // Pattern 3: sources.push({file:"..."})
      /sources\.push\s*\(\s*\{\s*file\s*:\s*["']([^"']+)["']/i,
      
      // Pattern 4: player.setup({sources:[{file:"..."}]})
      /player\.setup\s*\(\s*\{[^}]*sources\s*:\s*\[\s*\{[^}]*file\s*:\s*["']([^"']+)["']/i,
      
      // Pattern 5: "sources":[{"file":"..."}]
      /"sources"\s*:\s*\[\s*\{[^}]*"file"\s*:\s*"([^"]+)"/i,
      
      // Pattern 6: video.src = "..."
      /video\.src\s*=\s*["']([^"']+)["']/i,
      
      // Pattern 7: data-video-src
      /data-video-src=["']([^"']+)["']/i,
      
      // Pattern 8: data-src for video tag
      /<video[^>]+src=["']([^"']+)["']/i,
      
      // Pattern 9: Double-encoded or escaped
      /\\\\"file\\\\":\\\\"([^\\]+)\\\\"/,
      
      // Pattern 10: CDN URL patterns
      /https:\/\/[a-z0-9.-]+\.voe[_-]sx[a-z0-9.-]*\/[^\s"'<>]+\.(mp4|m3u8|webm|mkv)/gi
    ];

    for (let i = 0; i < patterns.length; i++) {
      const pattern = patterns[i];
      const matches = typeof pattern === 'string' ? html.match(new RegExp(pattern, 'gi')) : html.match(pattern);
      
      if (matches) {
        console.log(`  ✅ Pattern ${i + 1} matched!`);
        
        if (Array.isArray(matches)) {
          if (typeof matches[0] === 'string' && matches[0].includes('http')) {
            // Full URL matches
            console.log(`     Found URLs:`);
            const uniqueUrls = [...new Set(matches)];
            uniqueUrls.forEach(url => console.log(`       - ${url}`));
            return uniqueUrls[0];
          } else {
            // Capture group matches
            console.log(`     Found: ${matches.slice(1).join(', ')}`);
            return matches[1];
          }
        }
      }
    }

    // If no pattern matched, save HTML for debugging
    console.log(`  ❌ No patterns matched. Saving HTML for debugging...`);
    const debugFile = path.join(__dirname, `debug_voe_${fileCode}_${Date.now()}.html`);
    fs.writeFileSync(debugFile, html);
    console.log(`     Saved to: ${debugFile}`);
    
    return null;

  } catch (error) {
    console.log(`  ❌ Error: ${error.message}`);
    if (error.response) {
      console.log(`     Status: ${error.response.status}`);
      console.log(`     Headers: ${JSON.stringify(error.response.headers, null, 2)}`);
    }
    return null;
  }
}

async function testApiFileInfo(fileCode) {
  console.log(`\n📋 Testing Voe.sx API /file/info for: ${fileCode}`);
  
  try {
    const response = await axios.get('https://voe.sx/api/file/info', {
      params: {
        key: API_KEY,
        file_code: fileCode
      },
      timeout: 10000
    });
    
    console.log(`  Response:`);
    console.log(JSON.stringify(response.data, null, 2));
    
    return response.data;
    
  } catch (error) {
    console.log(`  ❌ API Error: ${error.message}`);
    return null;
  }
}

async function testProxyEndpoint() {
  console.log(`\n🌐 Testing proxy endpoint with Voe.sx...`);
  
  const testUrl = 'https://voe.sx/e/test123';
  const proxyUrl = `http://localhost:3000/api/proxy/video?url=${encodeURIComponent(testUrl)}`;
  
  try {
    // This will likely fail without proper setup, but tests if endpoint exists
    const response = await axios.get(proxyUrl, {
      timeout: 5000,
      validateStatus: () => true
    });
    
    console.log(`  Status: ${response.status}`);
    if (response.status === 200) {
      console.log(`  ✅ Proxy works!`);
    } else {
      console.log(`  ❌ Proxy failed: ${response.data?.error || 'Unknown error'}`);
    }
    
  } catch (error) {
    console.log(`  ❌ Cannot reach proxy: ${error.message}`);
    console.log(`     (This is expected if server is not running)`);
  }
}

async function main() {
  console.log('========================================');
  console.log('  Voe.sx Video URL Extraction Test');
  console.log('========================================');
  
  // Test proxy endpoint
  await testProxyEndpoint();
  
  // Test API file info (if key provided)
  if (API_KEY !== 'your_api_key_here') {
    console.log('\n📡 Testing Voe.sx API (authenticated)...');
    await testApiFileInfo(TEST_FILE_CODES[0]);
  } else {
    console.log('\n⚠️  Set VOE_API_KEY env var to test API');
  }
  
  // Test embed page scraping
  console.log('\n🎬 Testing embed page scraping...');
  for (const fileCode of TEST_FILE_CODES) {
    await extractVoeVideoUrl(fileCode);
  }
  
  console.log('\n========================================');
  console.log('  Test Complete');
  console.log('========================================');
  console.log('\n📝 Next steps:');
  console.log('  1. Run with real file codes');
  console.log('  2. If extraction fails, check debug HTML files');
  console.log('  3. Update patterns based on actual page structure');
}

main().catch(console.error);
