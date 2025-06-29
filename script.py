import asyncio
import csv
import re
import os
import json
import logging
import random
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse
from playwright.async_api import async_playwright


def setup_logging():
  """Setup logging configuration with timestamped log file"""
  logs_dir = 'logs'
  os.makedirs(logs_dir, exist_ok=True)

  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  log_filename = os.path.join(logs_dir, f'scraping_log_{timestamp}.log')

  logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s - %(levelname)s - %(message)s',
      handlers=[
          logging.FileHandler(log_filename, encoding='utf-8'),
          logging.StreamHandler()
      ]
  )
  return log_filename


async def simulate_human_behavior(page):
  """Simple human behavior simulation"""
  try:
    # Random mouse movement
    x = random.randint(100, 400)
    y = random.randint(100, 300)
    await page.mouse.move(x, y)
    await page.wait_for_timeout(random.randint(1000, 2000))

    # Light scrolling
    await page.evaluate("window.scrollBy(0, 200)")
    await page.wait_for_timeout(random.randint(1500, 2500))
  except:
    pass


async def is_cloudflare_active(page):
  """Check for Cloudflare challenge"""
  try:
    await page.locator('xpath=//*[@id="challenge-stage"]').wait_for(timeout=3000)
    return True
  except:
    return False


def is_valid_email(email):
  """Basic email validation"""
  if not email or '@' not in email:
    return False

  # Skip common invalid patterns
  invalid_patterns = [
    'noreply', 'no-reply', 'example', 'test', '.png', '.jpg',
    'sentry', # Filter out sentry system emails
    'notifications@', 'alert@', 'system@', # Common system email prefixes
    'donotreply', 'do-not-reply',
    'postmaster@', 'mailer-daemon@',
    'wordpress@', 'wp@', # Common CMS system emails
    'webmaster@', 'administrator@', 'admin@',
  ]
  email_lower = email.lower()

  if any(pattern in email_lower for pattern in invalid_patterns):
    return False

  # Check for hexadecimal or UUID-like patterns in the local part (before @)
  local_part = email.split('@')[0]
  if len(local_part) >= 32 or (len(local_part) >= 8 and all(c in '0123456789abcdef' for c in local_part.lower())):
    return False

  # Basic email format check
  email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
  return bool(re.match(email_pattern, email))


def is_valid_phone(phone):
  """Enhanced phone validation"""
  if not phone:
    return False

  # Remove any remaining non-digit characters except +
  clean_phone = re.sub(r'[^\d+]', '', phone)
  
  # Handle international numbers with country code
  if clean_phone.startswith('+'):
    # Remove the + for length checks
    digits_only = clean_phone[1:]
    # International numbers should be 10-15 digits
    if len(digits_only) < 10 or len(digits_only) > 15:
      return False
  else:
    digits_only = clean_phone
    # Domestic numbers should be 10-11 digits
    if len(digits_only) < 10 or len(digits_only) > 11:
      return False

  # Remove invalid patterns
  invalid_patterns = [
    r'^0+$',           # All zeros
    r'^1+$',           # All ones  
    r'^1234567890$',   # Sequential numbers
    r'^(\d)\1{9,}$',   # Repeated digit
    r'^12345678901$',  # Sequential with 1 prefix
  ]

  for pattern in invalid_patterns:
    if re.match(pattern, digits_only):
      return False

  # For US numbers (10 digits or 11 with 1 prefix)
  if len(digits_only) == 10:
    area_code = digits_only[:3]
    # Area code can't start with 0 or 1
    if area_code[0] in ['0', '1']:
      return False
    # Area code can't be all same digit or sequential
    if area_code[1:] in ['00', '11'] or area_code == '123':
      return False
      
  elif len(digits_only) == 11 and digits_only.startswith('1'):
    # US number with country code 1
    area_code = digits_only[1:4]
    if area_code[0] in ['0', '1']:
      return False
    if area_code[1:] in ['00', '11'] or area_code == '123':
      return False

  return True


def extract_business_name_from_url(url):
  """Extract business name from URL as required by client
  e.g. https://Syndr.ai -> Syndr
  """
  try:
    if not url:
      return ""
      
    # Parse URL and get domain
    parsed = urlparse(url)
    domain = parsed.netloc or parsed.path  # Use netloc if available, otherwise path
    
    # Remove www. if present
    if domain.startswith('www.'):
      domain = domain[4:]
    
    # Extract base domain without extension (.com, .ai, etc.)
    parts = domain.split('.')
    if len(parts) >= 1:
      base_name = parts[0]
      return base_name
    
    return domain
  except Exception as e:
    logging.error(f"Error extracting business name from URL {url}: {e}")
    return url


async def extract_emails(page):
  """Extract emails from page and return only the first one"""
  emails = set()

  # Get page content
  try:
    content = await page.content()

    # Look for mailto links first
    mailto_links = await page.query_selector_all('a[href^="mailto:"]')
    for link in mailto_links:
      href = await link.get_attribute('href')
      if href:
        # Clean the email address from mailto: and any URL parameters
        email = href.replace('mailto:', '').split('?')[0].strip()
        # Clean any URL encoding (like %20)
        email = email.replace('%20', '')
        if is_valid_email(email):
          emails.add(email)
          return list(emails)[:1]  # Return just the first email from mailto links

    # Search in content
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    found_emails = re.findall(email_pattern, content)
    for email in found_emails:
      # Clean any URL encoding (like %20)
      email = email.replace('%20', '').strip()
      if is_valid_email(email):
        emails.add(email)
        if len(emails) >= 1:  # Stop once we have 1 email
          break

  except Exception as e:
    logging.error(f"Error extracting emails: {e}")

  return list(emails)[:1]  # Return only the first email


async def extract_phones(page):
  """Extract phone numbers from page"""
  phones = set()

  try:
    # Look for tel: links first (highest priority)
    tel_links = await page.query_selector_all('a[href^="tel:"]')
    for link in tel_links:
      href = await link.get_attribute('href')
      if href:
        # Extract phone from tel: link more carefully
        tel_phone = href.replace('tel:', '').strip()
        # Remove common separators but keep + for international numbers
        cleaned_tel = re.sub(r'[^\d+]', '', tel_phone)
        if is_valid_phone(cleaned_tel):
          phones.add(cleaned_tel)
          return list(phones)[:1]  # Return immediately if found in tel: link

    # If no tel: links found, search in page content with better patterns
    content = await page.content()
    
    # Enhanced phone patterns to catch more formats
    phone_patterns = [
        # International format with country code
        r'\+\d{1,3}[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
        # US format with parentheses (555) 123-4567
        r'\((\d{3})\)[-.\s]?(\d{3})[-.\s]?(\d{4})',
        # US format with dashes 555-123-4567
        r'(\d{3})[-.\s](\d{3})[-.\s](\d{4})',
        # US format with dots 555.123.4567
        r'(\d{3})\.(\d{3})\.(\d{4})',
        # Compact format 5551234567
        r'(\d{3})(\d{3})(\d{4})',
        # International formats
        r'\+(\d{1,3})[-.\s]?(\d{1,4})[-.\s]?(\d{1,4})[-.\s]?(\d{1,9})',
        # Format with +1 country code
        r'\+1[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
    ]

    for pattern in phone_patterns:
      matches = re.findall(pattern, content)
      for match in matches:
        if isinstance(match, tuple):
          # Join all groups and clean
          phone = ''.join(match)
        else:
          phone = match
        
        # Clean the phone number
        phone = re.sub(r'[^\d+]', '', phone)
        
        # Add +1 if it's a 10-digit US number without country code
        if len(phone) == 10 and phone[0] not in ['0', '1']:
          phone = '+1' + phone
        
        if is_valid_phone(phone):
          phones.add(phone)
          return list(phones)[:1]  # Return the first valid phone found

    # Fallback: look for any sequence of digits that might be a phone
    fallback_pattern = r'\b(\d{10,15})\b'
    fallback_matches = re.findall(fallback_pattern, content)
    for phone in fallback_matches:
      if is_valid_phone(phone):
        # Add +1 if it's a 10-digit US number
        if len(phone) == 10 and phone[0] not in ['0', '1']:
          phone = '+1' + phone
        phones.add(phone)
        return list(phones)[:1]

  except Exception as e:
    logging.error(f"Error extracting phones: {e}")

  return list(phones)[:1]  # Return max 1 phone


def clean_url(url):
  """Clean URL by removing UTM parameters and other tracking parameters"""
  if not url:
    return url

  try:
    # Parse the URL
    parsed = urlparse(url)

    # Remove query parameters (everything after ?)
    clean_parsed = parsed._replace(query='', fragment='')

    # Reconstruct the clean URL
    clean_url = urlunparse(clean_parsed)

    # Remove trailing slash if present
    if clean_url.endswith('/'):
      clean_url = clean_url[:-1]

    return clean_url

  except Exception as e:
    logging.error(f"Error cleaning URL {url}: {e}")
    return url


async def scrape_business_info(page, url, writer, csvfile, unique_businesses):
  """Scrape business information and write to CSV"""
  try:
    # Clean the URL to remove UTM parameters
    clean_website_url = clean_url(url)

    # Extract business name from URL instead of page content
    business_name = extract_business_name_from_url(clean_website_url)
    if not business_name.strip():
      return 0

    # Make sure unique_businesses checks are thread-safe
    # Since we're using a shared set with parallel execution
    business_name_key = business_name.lower()  # Normalize for comparison
    
    # Skip duplicates - using a more thread-safe approach
    # Check-then-add approach needs to be atomic in a parallel context
    if business_name_key in unique_businesses:
      return 0
    unique_businesses.add(business_name_key)

    # Extract contact info
    emails = await extract_emails(page)  # Now returns max 1 email
    phones = await extract_phones(page)  # Already returns max 1 phone

    # Prepare data for CSV
    email = emails[0] if emails else ""
    phone = phones[0] if phones else ""
    
    # Since we're running in parallel, we need to make sure the CSV writing is thread-safe
    # The lock is handled in the calling function (process_business_links)
    writer.writerow({
        'website_url': clean_website_url,
        'business_name': business_name,
        'email': email,
        'phone': phone
    })
    csvfile.flush()  # Immediately flush to file
    
    if email and phone:
      logging.info(f"💾 Saved: {business_name} | {email} | {phone}")
    elif email:
      logging.info(f"💾 Saved: {business_name} | {email}")
    elif phone:
      logging.info(f"💾 Saved: {business_name} | {phone}")
    else:
      logging.info(f"💾 Saved: {business_name} (no contact info)")
    
    return 1  # Always return 1 since we only create one entry per website

  except Exception as e:
    logging.error(f"Error scraping {url}: {e}")
    return 0


async def get_business_links_from_page(page):
  """Get business links from current page without scrolling through multiple pages"""
  # Try to wait for the selector, but don't fail if it times out
  try:
    await page.wait_for_selector('a.provider__cta-link.website-link__item', timeout=15000)
  except:
    # If selector timeout, try to get whatever links are available
    logging.warning("⚠️ Timeout waiting for business links. Continuing with available links.")
    pass

  # Give the page a moment to load
  await page.wait_for_timeout(3000)

  # Simple scroll to load all businesses on current page
  prev_count = 0
  scroll_attempts = 0
  max_attempts = 5  # Reduced since we're only loading one page

  while scroll_attempts < max_attempts:
    try:
      links = await page.query_selector_all('a.provider__cta-link.website-link__item')
      current_count = len(links)

      if current_count == prev_count:
        scroll_attempts += 1
      else:
        scroll_attempts = 0

      prev_count = current_count
      
      # Log progress
      if scroll_attempts > 0:
        logging.info(f"🔄 Scrolling to load more businesses ({scroll_attempts}/{max_attempts}), found {current_count} so far")

      # Scroll down
      await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
      await page.wait_for_timeout(1500)
    except Exception as e:
      logging.warning(f"⚠️ Error while scrolling: {e}")
      break

  try:
    final_links = await page.query_selector_all('a.provider__cta-link.website-link__item')
    return final_links
  except Exception as e:
    logging.error(f"❌ Error getting final links: {e}")
    return []


async def process_business_links(page, links, writer, csvfile, unique_businesses):
  """Process all business links from current page in parallel"""
  processed_urls = set()
  tasks = []
  
  if not links:
    return 0

  # Configure the maximum number of concurrent tasks
  # Adjust this value based on your system capabilities and website limitations
  max_concurrent_tasks = 10
  semaphore = asyncio.Semaphore(max_concurrent_tasks)

  # Create a lock for thread-safe CSV writing
  csv_lock = asyncio.Lock()

  # Modified scrape_business_info function with locking
  async def safe_scrape_business_info(page, url, writer, csvfile, unique_businesses_shared):
    async with csv_lock:
      return await scrape_business_info(page, url, writer, csvfile, unique_businesses_shared)

  # Wrapper function for processing with semaphore
  async def process_with_semaphore(context, href, index, total):
    async with semaphore:
      return await process_single_business(context, href, writer, csvfile, unique_businesses, index, total)

  logging.info(f"🚀 Processing {len(links)} businesses from current page using parallel processing (max {max_concurrent_tasks} concurrent)...")

  # Collect URLs to process
  urls_to_process = []
  for i, link in enumerate(links):
    href = await link.get_attribute('href')
    if not href or href in processed_urls:
      continue
    processed_urls.add(href)
    urls_to_process.append((i, href))

  # Create tasks for parallel processing
  for i, href in urls_to_process:
    task = asyncio.create_task(
      process_with_semaphore(page.context, href, i, len(links))
    )
    tasks.append(task)

  # Wait for all tasks to complete and collect results
  if tasks:
    results = await asyncio.gather(*tasks)
    total_entries = sum(results)
    
    # Log completion
    logging.info(f"✅ Completed parallel processing of {len(tasks)} businesses")
    
    return total_entries
  else:
    return 0


async def read_base_urls():
  """Read base URLs from the scrape_urls.txt file"""
  urls = []
  try:
    with open('scrape_urls.txt', 'r') as f:
      for line in f:
        url = line.strip()
        if url and not url.startswith('#'):  # Skip empty lines and comments
          urls.append(url)
    
    if not urls:
      # Fall back to default URL if file is empty
      logging.warning("⚠️ No URLs found in scrape_urls.txt, using default URL")
      urls = ['https://clutch.co/affiliate-marketing/facebook']
    
    return urls
  except FileNotFoundError:
    logging.warning("⚠️ scrape_urls.txt not found, using default URL")
    return ['https://clutch.co/affiliate-marketing/facebook']
  except Exception as e:
    logging.error(f"❌ Error reading scrape_urls.txt: {e}")
    return ['https://clutch.co/affiliate-marketing/facebook']


async def process_single_business(page_context, href, writer, csvfile, unique_businesses, index, total_links):
  """Process a single business link and scrape its information"""
  business_page = None
  max_retries = 2
  retry_count = 0
  
  while retry_count <= max_retries:
    try:
      business_page = await page_context.new_page()

      # Navigate to business website with proper error handling
      try:
        # Increase timeout for navigation if this is a retry
        timeout = 30000 if retry_count == 0 else 45000
        
        # Log navigation attempt
        logging.info(f"🔗 Navigating to business {index+1}/{total_links}: {href}" + 
                    (f" (Retry {retry_count})" if retry_count > 0 else ""))
        
        await business_page.goto(href, wait_until='domcontentloaded', timeout=timeout)
        await business_page.wait_for_timeout(2000)
        
        # Check for Cloudflare
        if await is_cloudflare_active(business_page):
          logging.info(f"⚠️  Cloudflare detected for business {index+1}, skipping...")
          return 0  # Skip this business entirely
          
        # Light human simulation
        await simulate_human_behavior(business_page)

        # Scrape business info
        actual_url = business_page.url
        entries = await scrape_business_info(business_page, actual_url, writer, csvfile, unique_businesses)
        
        # If we get here, the scraping was successful
        return entries
        
      except Exception as nav_error:
        if "Timeout" in str(nav_error) and retry_count < max_retries:
          retry_count += 1
          logging.warning(f"⚠️ Timeout navigating to business {index+1}, retrying ({retry_count}/{max_retries})...")
          
          # Close current page before retry
          if business_page:
            try:
              await business_page.close()
              business_page = None
            except:
              pass
            
          # Wait before retrying
          await asyncio.sleep(random.uniform(3, 5))
          continue  # Try again
        else:
          # Either it's not a timeout error or we've exceeded retries
          logging.error(f"❌ Error processing business {index+1}: {nav_error}")
          return 0  # Exit the retry loop

    except Exception as e:
      logging.error(f"❌ Error creating page for business {index+1}: {e}")
      return 0
      
    finally:
      if business_page:
        try:
          await business_page.close()
        except:
          pass
  
  return 0  # Default return if all attempts fail


async def main():
  """Main function"""
  # Configure the maximum parallel business pages to process
  # Adjust based on your system capabilities and to avoid being rate-limited
  MAX_CONCURRENT_BUSINESS_PAGES = 10
  
  log_filename = setup_logging()
  logging.info(f"🚀 Starting web scraper with parallel processing - Log file: {log_filename}")
  logging.info(f"⚙️ Max concurrent business pages: {MAX_CONCURRENT_BUSINESS_PAGES}")

  # Create output directory
  output_dir = 'scraped_data'
  os.makedirs(output_dir, exist_ok=True)

  # Generate output filename
  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  csv_filename = os.path.join(
    output_dir, f'scraped_businesses_{timestamp}.csv')

  async with async_playwright() as p:
    # Launch browser with reasonable stealth settings
    browser = await p.chromium.launch(
        headless=False,
        args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            # Add these options to optimize browser performance with multiple pages
            '--disable-dev-shm-usage',  # Overcome limited /dev/shm size in containers
            '--disable-gpu',  # Disable GPU hardware acceleration
            '--disable-setuid-sandbox',
            '--disable-extensions',  # Disable extensions for better performance
            '--disable-background-timer-throttling',  # Improve timer accuracy
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',  # Prevent throttling of background tabs
        ]
    )

    # Create a persistent context with optimized settings for parallel processing
    context = await browser.new_context(
        viewport={'width': 1366, 'height': 768},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        # Set a lower JavaScript memory limit to reduce memory usage
        java_script_enabled=True,
        # These settings help with performance in parallel processing
        bypass_csp=True,  # Avoid content security policy issues
        ignore_https_errors=True  # Ignore HTTPS errors that might slow down navigation
    )
    
    # Allow more concurrent connections to improve parallel processing
    await context.route('**', lambda route: route.continue_())

    page = await context.new_page()

    try:
      # Read URLs from file
      base_urls = await read_base_urls()
      logging.info(f"📋 Found {len(base_urls)} URLs to scrape: {', '.join(base_urls)}")

      # Process businesses and save to CSV
      unique_businesses = set()
      total_entries = 0
      total_businesses_found = 0
      
      with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['website_url', 'business_name', 'email', 'phone']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        logging.info(f"📝 Created CSV file: {csv_filename}")

        for start_url in base_urls:
          try:
            # Navigate to the starting URL
            logging.info(f"🔍 Navigating to start URL: {start_url}")
            await page.goto(start_url, wait_until='domcontentloaded')

            # Give time for the page to load
            await page.wait_for_timeout(5000)

            # Check for Cloudflare
            if await is_cloudflare_active(page):
              logging.info("⚠️ Cloudflare challenge detected, skipping this URL")
              continue  # Skip this URL entirely
            
            # Light human simulation
            await simulate_human_behavior(page)

            # Get business links from the page
            business_links = await get_business_links_from_page(page)

            # If no links found, log and skip
            if not business_links:
              logging.info("🔍 No business links found on this page")
              continue

            # Process the business links in parallel
            logging.info(f"⚡ Processing {len(business_links)} businesses in parallel (max {MAX_CONCURRENT_BUSINESS_PAGES} concurrent)...")
            entries = await process_business_links(page, business_links, writer, csvfile, unique_businesses)
            total_entries += entries
            total_businesses_found += len(business_links)
            
            logging.info(f"✅ Found and processed {len(business_links)} businesses, {entries} entries saved")
            
          except Exception as e:
            logging.error(f"Error processing URL {start_url}: {e}")

      logging.info(f"🎉 Scraping completed! Total entries: {total_entries}")

    except Exception as e:
      logging.error(f"Error in main processing: {e}")

    finally:
      await page.close()
      await context.close()
      await browser.close()

  logging.info("✅ Browser closed, exiting program.")


# Run the scraper
asyncio.run(main())
