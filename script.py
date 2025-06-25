import asyncio
import csv
import re
import os
import json
import logging
import random
from datetime import datetime
from urllib.parse import urljoin
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
  invalid_patterns = ['noreply', 'no-reply', 'example', 'test', '.png', '.jpg']
  email_lower = email.lower()

  if any(pattern in email_lower for pattern in invalid_patterns):
    return False

  # Basic email format check
  email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
  return bool(re.match(email_pattern, email))


def is_valid_phone(phone):
  """Basic phone validation"""
  if not phone or len(phone) < 10:
    return False

  # Remove invalid patterns
  invalid_patterns = [r'^0+$', r'^1+$', r'^1234567890$', r'^(\d)\1{9,}$']

  for pattern in invalid_patterns:
    if re.match(pattern, phone):
      return False

  # Check for reasonable phone format
  if len(phone) == 10:
    area_code = phone[:3]
    if area_code[0] in ['0', '1'] or area_code[1:] in ['00', '11']:
      return False

  return True


async def extract_business_name(page):
  """Extract business name using multiple strategies"""
  # Try title tag first
  try:
    title = await page.title()
    if title and len(title.strip()) < 100:
      # Clean up title
      title_clean = re.sub(
        r'(\s*[-|]\s*(Home|Homepage|Welcome).*)', '', title, flags=re.IGNORECASE)
      return title_clean.strip()
  except:
    pass

  # Try h1 tag
  try:
    h1 = await page.query_selector('h1')
    if h1:
      h1_text = await h1.text_content()
      if h1_text and len(h1_text.strip()) < 100:
        return h1_text.strip()
  except:
    pass

  # Try meta og:site_name
  try:
    og_title = await page.query_selector('meta[property="og:site_name"]')
    if og_title:
      content = await og_title.get_attribute('content')
      if content:
        return content.strip()
  except:
    pass

  return ""


async def extract_emails(page):
  """Extract emails from page"""
  emails = set()

  # Get page content
  try:
    content = await page.content()

    # Look for mailto links first
    mailto_links = await page.query_selector_all('a[href^="mailto:"]')
    for link in mailto_links:
      href = await link.get_attribute('href')
      if href:
        email = href.replace('mailto:', '').split('?')[0]
        if is_valid_email(email):
          emails.add(email)

    # Search in content
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    found_emails = re.findall(email_pattern, content)
    for email in found_emails:
      if is_valid_email(email):
        emails.add(email)

  except Exception as e:
    logging.error(f"Error extracting emails: {e}")

  return list(emails)


async def extract_phones(page):
  """Extract phone numbers from page"""
  phones = set()

  try:
    # Look for tel: links first
    tel_links = await page.query_selector_all('a[href^="tel:"]')
    for link in tel_links:
      href = await link.get_attribute('href')
      if href:
        phone = re.sub(r'[^\d+]', '', href.replace('tel:', ''))
        if is_valid_phone(phone):
          phones.add(phone)

    # Search in page content
    content = await page.content()
    phone_patterns = [
        r'\+?1?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})',
        r'(\d{3})[-.\s]?(\d{3})[-.\s]?(\d{4})',
    ]

    for pattern in phone_patterns:
      matches = re.findall(pattern, content)
      for match in matches:
        if isinstance(match, tuple):
          phone = ''.join(match)
        else:
          phone = match
        phone = re.sub(r'[^\d+]', '', phone)
        if is_valid_phone(phone):
          phones.add(phone)
          break  # Only get the first valid phone

  except Exception as e:
    logging.error(f"Error extracting phones: {e}")

  return list(phones)[:1]  # Return max 1 phone


async def scrape_business_info(page, url, writer, csvfile, unique_businesses):
  """Scrape business information and write to CSV"""
  try:
    # Clean the URL to remove UTM parameters
    clean_website_url = clean_url(url)

    # Extract business name
    business_name = await extract_business_name(page)
    if not business_name.strip():
      return 0

    # Skip duplicates
    if business_name in unique_businesses:
      return 0
    unique_businesses.add(business_name)

    # Extract contact info
    emails = await extract_emails(page)
    phones = await extract_phones(page)

    # Write to CSV
    if not emails and not phones:
      # Write entry with no contact info
      writer.writerow({
          'website_url': clean_website_url,
          'business_name': business_name,
          'email': '',
          'phone': ''
      })
      csvfile.flush()  # Immediately flush to file
      logging.info(f"💾 Saved: {business_name} (no contact info)")
      return 1

    entries_written = 0
    # Create entries for each email/phone combination
    if emails and phones:
      for email in emails:
        for phone in phones:
          writer.writerow({
              'website_url': clean_website_url,
              'business_name': business_name,
              'email': email,
              'phone': phone
          })
          csvfile.flush()  # Immediately flush to file
          entries_written += 1
          logging.info(f"💾 Saved: {business_name} | {email} | {phone}")
    elif emails:
      for email in emails:
        writer.writerow({
            'website_url': clean_website_url,
            'business_name': business_name,
            'email': email,
            'phone': ''
        })
        csvfile.flush()  # Immediately flush to file
        entries_written += 1
        logging.info(f"💾 Saved: {business_name} | {email}")
    elif phones:
      for phone in phones:
        writer.writerow({
            'website_url': clean_website_url,
            'business_name': business_name,
            'email': '',
            'phone': phone
        })
        csvfile.flush()  # Immediately flush to file
        entries_written += 1
        logging.info(f"💾 Saved: {business_name} | {phone}")

    return entries_written

  except Exception as e:
    logging.error(f"Error scraping {url}: {e}")
    return 0


async def get_business_links_from_page(page):
  """Get business links from current page without scrolling through multiple pages"""
  # Wait for initial load
  await page.wait_for_selector('a.provider__cta-link.website-link__item', timeout=30000)

  # Simple scroll to load all businesses on current page
  prev_count = 0
  scroll_attempts = 0
  max_attempts = 5  # Reduced since we're only loading one page

  while scroll_attempts < max_attempts:
    links = await page.query_selector_all('a.provider__cta-link.website-link__item')
    current_count = len(links)

    if current_count == prev_count:
      scroll_attempts += 1
    else:
      scroll_attempts = 0

    prev_count = current_count

    # Scroll down
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await page.wait_for_timeout(1500)

  final_links = await page.query_selector_all('a.provider__cta-link.website-link__item')
  return final_links


async def process_business_links(page, links, writer, csvfile, unique_businesses):
  """Process all business links from current page"""
  total_entries = 0
  processed_urls = set()

  if not links:
    return 0

  logging.info(f"🚀 Processing {len(links)} businesses from current page...")

  for i, link in enumerate(links):
    href = await link.get_attribute('href')
    if not href or href in processed_urls:
      continue

    processed_urls.add(href)

    # Log progress every 5 businesses for more frequent updates
    if i % 5 == 0 or i == len(links) - 1:
      logging.info(f"📊 Page progress: {i+1}/{len(links)} businesses processed")

    # Create new page for each business
    business_page = None
    try:
      business_page = await page.context.new_page()

      # Navigate to business website
      await business_page.goto(href, wait_until='networkidle', timeout=30000)
      await business_page.wait_for_timeout(2000)

      # Check for Cloudflare
      if await is_cloudflare_active(business_page):
        logging.info(
          f"⚠️  Cloudflare detected for business {i+1}, skipping...")
        continue

      # Light human simulation
      await simulate_human_behavior(business_page)

      # Scrape business info
      actual_url = business_page.url
      entries = await scrape_business_info(business_page, actual_url, writer, csvfile, unique_businesses)
      total_entries += entries

    except Exception as e:
      logging.error(f"❌ Error processing business {i+1}: {e}")
    finally:
      if business_page:
        try:
          await business_page.close()
        except:
          pass

    # Small delay between businesses
    await asyncio.sleep(random.uniform(1, 3))

  return total_entries


async def main():
  """Main function"""
  log_filename = setup_logging()
  logging.info(f"🚀 Starting web scraper - Log file: {log_filename}")

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
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
    )

    context = await browser.new_context(
        viewport={'width': 1366, 'height': 768},
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    page = await context.new_page()

    try:
      # Base URL for pagination
      base_url = 'https://clutch.co/affiliate-marketing/facebook'

      # Process businesses and save to CSV
      unique_businesses = set()
      total_entries = 0
      total_businesses_found = 0
      current_page = 1

      with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['website_url', 'business_name', 'email', 'phone']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        logging.info(f"📝 Created CSV file: {csv_filename}")

        # Process pages sequentially
        while True:
          # Construct URL for current page
          if current_page == 1:
            page_url = base_url
          else:
            page_url = f"{base_url}?page={current_page}"

          logging.info(f"🌐 Processing page {current_page}: {page_url}")

          try:
            # Navigate to the page
            await page.goto(page_url, wait_until='networkidle', timeout=60000)
            await page.wait_for_timeout(2000)

            # Initial human behavior
            await simulate_human_behavior(page)

            # Get business links from current page
            links = await get_business_links_from_page(page)

            # If no businesses found, we've reached the end
            if not links:
              logging.info(
                f"❌ No businesses found on page {current_page}. Pagination complete.")
              break

            logging.info(
              f"📋 Found {len(links)} businesses on page {current_page}")
            total_businesses_found += len(links)

            # Process businesses from this page
            page_entries = await process_business_links(page, links, writer, csvfile, unique_businesses)
            total_entries += page_entries

            logging.info(
              f"✅ Completed page {current_page}. Entries from this page: {page_entries}")

            # Move to next page
            current_page += 1

            # Add delay between pages to be respectful
            await asyncio.sleep(random.uniform(2, 4))

          except Exception as e:
            logging.error(f"❌ Error processing page {current_page}: {e}")
            # If we get an error, it might mean we've reached the end of pages
            break

      # Final summary
      logging.info(f"\n" + "=" * 60)
      logging.info(f"✅ SCRAPING COMPLETED SUCCESSFULLY")
      logging.info(f"=" * 60)
      logging.info(f"📄 Total pages processed: {current_page - 1}")
      logging.info(f"🏢 Total businesses found: {total_businesses_found}")
      logging.info(f"📊 Total entries written: {total_entries}")
      logging.info(f"🔍 Unique businesses: {len(unique_businesses)}")
      logging.info(f"💾 Data saved to: {csv_filename}")
      logging.info(f"=" * 60)

      logging.info(
        f"🎉 Scraping completed successfully! Total pages: {current_page - 1}, Total entries: {total_entries}")

    except Exception as e:
      logging.error(f"Main error: {e}")
      logging.error(f"Error: {e}")
    finally:
      await asyncio.sleep(5)  # Brief pause before closing
      await browser.close()
      logging.info("Browser closed.")


def clean_url(url):
  """Clean URL by removing UTM parameters and other tracking parameters"""
  if not url:
    return url

  try:
    from urllib.parse import urlparse, urlunparse, parse_qs

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


if __name__ == '__main__':
  asyncio.run(main())
