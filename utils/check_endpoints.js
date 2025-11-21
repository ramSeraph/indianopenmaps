const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');
const routes = require('./server/routes.json');

const BASE_URL = 'http://localhost:3000';

async function checkEndpoint(path) {
  const tilesJsonUrl = `${BASE_URL}${path}tiles.json`;
  
  try {
    const response = await fetch(tilesJsonUrl);
    
    if (!response.ok) {
      return {
        path,
        error: `HTTP ${response.status}: ${response.statusText}`,
        success: false
      };
    }
    
    const data = await response.json();
    
    return {
      path,
      success: true
    };
  } catch (error) {
    return {
      path,
      error: error.message,
      success: false
    };
  }
}

function loadPathsFromFile(filepath) {
  try {
    const content = fs.readFileSync(filepath, 'utf-8');
    return content
      .split('\n')
      .map(line => line.trim())
      .filter(line => line.length > 0 && !line.startsWith('#'));
  } catch (error) {
    console.error(`Error reading file ${filepath}:`, error.message);
    process.exit(1);
  }
}

async function checkAllEndpoints(inputFile) {
  let endpoints;
  
  if (inputFile) {
    console.log(`Checking endpoints from file: ${inputFile}\n`);
    endpoints = loadPathsFromFile(inputFile);
  } else {
    console.log('Checking all endpoints from routes.json...\n');
    endpoints = Object.keys(routes);
  }
  
  const results = [];
  const errors = [];
  
  // Check endpoints one by one to avoid overwhelming the server
  for (const endpointPath of endpoints) {
    const result = await checkEndpoint(endpointPath);
    results.push(result);
    
    if (!result.success) {
      errors.push(result);
      console.log(`✗ ${result.path} - ${result.error}`);
    } else {
      console.log(`✓ ${result.path}`);
    }
    
    // Small delay to avoid overwhelming the server
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  
  console.log('\n=== SUMMARY ===');
  console.log(`Total endpoints: ${results.length}`);
  console.log(`Successful: ${results.filter(r => r.success).length}`);
  console.log(`Failed: ${errors.length}`);
  
  if (errors.length > 0) {
    console.log('\n=== FAILED ENDPOINTS ===');
    errors.forEach(err => {
      console.log(`${err.path}`);
      console.log(`  Error: ${err.error}\n`);
    });
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
const inputFile = args[0];

if (args.length > 1) {
  console.error('Usage: node check_endpoints.js [input_file]');
  console.error('  input_file: Optional file containing paths to check (one per line)');
  process.exit(1);
}

// Run the check
checkAllEndpoints(inputFile).catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
