import asyncio
import csv
import re
import os
import json
import logging
import random
import sys
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse
from playwright.async_api import async_playwright

max_concurrent = 50

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
  """Enhanced human behavior simulation"""
  try:
    # Random mouse movement
    x = random.randint(100, 700)  # Wider range for more natural movement
    y = random.randint(100, 500)  # Taller range
    await page.mouse.move(x, y)
    await page.wait_for_timeout(random.randint(1000, 2500))
    
    # Sometimes move twice for more natural behavior
    if random.random() > 0.5:
      x2 = random.randint(200, 600)
      y2 = random.randint(150, 450)
      await page.mouse.move(x2, y2, steps=5)  # Add steps for smoother movement
      await page.wait_for_timeout(random.randint(800, 1800))
    
    # Variable scrolling pattern
    scroll_distance = random.randint(150, 400)
    await page.evaluate(f"window.scrollBy(0, {scroll_distance})")
    await page.wait_for_timeout(random.randint(1000, 2000))
    
    # Sometimes scroll back up slightly
    if random.random() > 0.7:
      up_distance = random.randint(50, 150)
      await page.evaluate(f"window.scrollBy(0, -{up_distance})")
      await page.wait_for_timeout(random.randint(700, 1500))
  except Exception as e:
    logging.warning(f"Error during human behavior simulation: {e}")
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

    # Skip duplicates
    if business_name in unique_businesses:
      return 0
    unique_businesses.add(business_name)

    # Extract contact info
    emails = await extract_emails(page)  # Now returns max 1 email
    phones = await extract_phones(page)  # Already returns max 1 phone

    # Prepare data for CSV
    email = emails[0] if emails else ""
    phone = phones[0] if phones else ""
    
    # Write to CSV - one row per website with at most one email and one phone
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
  """Get business links from the current page by scrolling down to load all lazy-loaded content, optimized for Clutch.co"""
  # Multiple possible selectors for business website links on Clutch.co
  link_selectors = [
    'a[href*="r.clutch.co/redirect"]', # Clutch.co Visit Website links
    'a.provider__cta-link.website-link__item',
    'a.website-link__item',
    'a.visit-website',  # Common on Clutch.co
    '.website-link',
    '.provider-row a[href^="http"]',
    '.provider-info a.provider__website',
    '.directory_profile a.website',
    '.company-details a[target="_blank"]',
    '.listing-item a.website'
  ]
  
  # OPTIMIZED: Skip initial complex scrolling - go straight to selector finding
  # Most links are visible immediately on modern Clutch.co pages
  found_selector = None
  
  # Try the various selectors to find business links immediately
  for selector in link_selectors:
    try:
      await page.wait_for_selector(selector, timeout=3000)  # Reduced timeout
      found_selector = selector
      logging.info(f"✅ Found business links using selector: {selector}")
      break
    except Exception:
      continue
  
  # We no longer use Clutch.co profile pages as fallback
  # Instead, if we don't have direct website links, try a more focused approach for "Visit Website" buttons
  if not found_selector:
    # If no predefined selector worked, check specifically for "Visit Website" links
    logging.warning("⚠️ No predefined business link selector found. Looking for 'Visit Website' links.")
    try:
      # Look specifically for "Visit Website" text in links
      visit_website_selector = 'a:has-text("Visit Website"), a[title*="Visit Website"], a.visit-website'
      await page.wait_for_selector(visit_website_selector, timeout=5000)
      found_selector = visit_website_selector
      logging.info(f"✅ Found 'Visit Website' links using: {visit_website_selector}")
    except Exception:
      # If still no luck, try generic external links as a last resort
      logging.warning("⚠️ No 'Visit Website' links found. Trying generic approach.")
      try:
        # Check if there are any external links on the page
        external_links_count = await page.evaluate('''() => {
          const links = Array.from(document.querySelectorAll('a[href^="http"]'));
          const externalLinks = links.filter(a => 
            !a.href.includes(window.location.hostname) && 
            !a.href.includes('facebook.com') && 
            !a.href.includes('twitter.com') && 
            !a.href.includes('linkedin.com') &&
            !a.href.includes('clutch.co/directories') &&
            !a.href.includes('clutch.co/profile') &&
            !a.href.includes('clutch.co/about-us') &&
            !a.href.includes('clutch.co/methodology')
          );
          return externalLinks.length;
        }''')
        
        if external_links_count > 0:
          logging.info(f"🔍 Found {external_links_count} generic external links")
          found_selector = 'a[href^="http"]:not([href*="clutch.co/profile"])'
        else:
          logging.warning("⚠️ No external links found on page")
      except Exception as e:
        logging.error(f"❌ Error when checking for generic links: {e}")
  
  # Give the page a minimal moment to load
  await page.wait_for_timeout(500)  # Reduced from 3000 for speed
  
  # OPTIMIZED: Fast scrolling with batch collection
  # Scroll to load all content in fewer attempts with larger increments
  scroll_attempts = 0
  max_attempts = 3  # Reduced from 10 for speed
  
  # Function to count links with valid selector
  async def count_links():
    if found_selector:
      try:
        links = await page.query_selector_all(found_selector)
        return len(links)
      except Exception:
        pass
    return 0
  
  # Initial count
  current_count = await count_links()
  
  # If no links found initially, try scrolling anyway to trigger lazy loading
  if current_count == 0:
    logging.info("⚠️ No links found initially, fast scrolling to trigger lazy loading...")
  
  # OPTIMIZED: Fast bulk scrolling without counting between scrolls
  try:
    # Get page dimensions for optimized scrolling
    page_height = await page.evaluate('document.body.scrollHeight')
    viewport_height = await page.evaluate('window.innerHeight')
    
    # Calculate how many large scrolls we need (fewer, larger scrolls)
    scroll_distance = viewport_height * 2  # Scroll 2 viewports at a time
    scroll_positions = []
    
    # Generate scroll positions to cover the entire page
    current_pos = 0
    while current_pos < page_height:
      scroll_positions.append(current_pos)
      current_pos += scroll_distance
    
    # Add final scroll to absolute bottom
    scroll_positions.append(page_height)
    
    logging.info(f"� Fast scrolling through {len(scroll_positions)} positions to load all content")
    
    # Execute all scrolls quickly without individual checks
    for i, pos in enumerate(scroll_positions):
      await page.evaluate(f"window.scrollTo(0, {pos})")
      # Minimal delay between scrolls for content loading
      await page.wait_for_timeout(200)  # Much faster than previous 1500-2500ms
      
      # Only log progress every few scrolls
      if i % 3 == 0 or i == len(scroll_positions) - 1:
        logging.info(f"� Fast scroll progress: {i+1}/{len(scroll_positions)}")
    
    # Final count after all scrolling is complete
    final_count = await count_links()
    logging.info(f"✅ Fast scrolling complete: Found {final_count} business links")
    
  except Exception as e:
    logging.warning(f"⚠️ Error during fast scrolling: {e}")
    # Fallback to simple scroll to bottom
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await page.wait_for_timeout(1000)
  
  # Final collection of all links
  if found_selector:
    try:
      # Get links with our found selector
      all_links = await page.query_selector_all(found_selector)
      logging.info(f"✅ Collected {len(all_links)} business links with selector: {found_selector}")
    except Exception as e:
      logging.error(f"❌ Error collecting links with selector {found_selector}: {e}")
  
  # If we didn't find many links with our main selectors, try only the "Visit Website" links
  if len(all_links) < 10:
    logging.info("⚠️ Found few links, trying Clutch.co redirect links approach...")

    try:
      # Only look for "Visit Website" redirect links which are common on Clutch.co
      website_links = await page.query_selector_all('a[href*="r.clutch.co/redirect"]')
      if website_links and len(website_links) > 0:
        logging.info(f"🔍 Found {len(website_links)} Clutch.co redirect links")
        all_links = website_links
    except Exception as e:
      logging.error(f"❌ Error using Clutch.co redirect links approach: {e}")
  
  # If we still don't have many links, try a more general approach
  if len(all_links) < 5:
    logging.info("⚠️ Still found few links, trying backup approach...")
    
    try:
      # Get all external links that could be business websites
      backup_links = await page.evaluate('''() => {
        const links = Array.from(document.querySelectorAll('a[href^="http"]'));
        const externalLinks = links.filter(a => 
          !a.href.includes(window.location.hostname) && 
          !a.href.includes('facebook.com') && 
          !a.href.includes('twitter.com') && 
          !a.href.includes('linkedin.com') &&
          !a.href.includes('google.com') &&
          !a.href.includes('clutch.co/directories') &&
          !a.href.includes('clutch.co/about-us') &&
          !a.href.includes('clutch.co/methodology')
        );
        return externalLinks;
      }''')
      
      if backup_links and len(backup_links) > 0:
        all_links = backup_links
        logging.info(f"🔍 Found {len(all_links)} business links using backup approach")
    except Exception as e:
      logging.error(f"❌ Error using backup approach: {e}")
    
  return all_links


async def has_next_page(page):
  """Enhanced check for next page button with multiple selectors specifically optimized for Clutch.co
  Returns the next button element if it exists and is not disabled, None otherwise."""
  selectors = [
    'a.page-item.next', # Main Clutch.co pagination next button
    'a.page-link[rel="next"]',
    'a[aria-label="Go to Next Page"]', # Clutch.co uses this
    '.pagination .page-item:last-child:not(.disabled) a', # Common Clutch.co pattern
    '.page-item.next:not(.disabled) a', # Another Clutch.co pattern
    'a[href*="page="]', # Links with page parameter
    'li.next a',
    'a.next',
    '.pagination a[aria-label="Next"]',
    'a[data-page="next"]',
    '.pagination-next a',
    '.pagination ul li:last-child a',
    '.pagination__next'
  ]
  
  # First, check for disabled next buttons (more accurate detection of end of pages)
  try:
    # Check for visible but disabled next buttons - if we find these, we're definitely at the last page
    disabled_selectors = [
      '.pagination .page-item.next.disabled',
      '.pagination .page-item.disabled:has(a:has-text("Next"))',
      'a.page-link[rel="next"][aria-disabled="true"]',
      '.pagination .page-item.disabled a[aria-label="Next"]',
      'li.pager-next.disabled',  # Common Drupal pagination used by Clutch.co
      'li.pager-item.pager-next.disabled'
    ]
    
    for selector in disabled_selectors:
      disabled_next = await page.query_selector(selector)
      if disabled_next and await disabled_next.is_visible():
        logging.info(f"🛑 Found disabled next button with selector: {selector} - Reached last page")
        return None  # We found a disabled next button - definitely at the last page
  except Exception as e:
    logging.warning(f"Error checking for disabled next buttons: {e}")
  
  # If we didn't find a disabled next button, look for an enabled one
  try:
    for selector in selectors:
      next_button = await page.query_selector(selector)
      if next_button:
        # Check if button is visible and not disabled
        is_visible = await next_button.is_visible()
        class_attr = await next_button.get_attribute('class') or ""
        parent_class = ""
        
        # Also check parent element for disabled class
        try:
          parent = await next_button.evaluate('(node) => node.parentElement')
          if parent:
            parent_class = await page.evaluate('(node) => node.getAttribute("class") || ""', parent)
        except:
          pass
          
        # Check both element and parent for disabled status
        is_disabled = ("disabled" in class_attr) or ("disabled" in parent_class)
        
        # Also check aria-disabled attribute
        aria_disabled = await next_button.get_attribute('aria-disabled')
        if aria_disabled and aria_disabled.lower() == "true":
          is_disabled = True
        
        # Check if href is empty or just "#" - often indicates disabled
        href = await next_button.get_attribute('href')
        if not href or href == "#":
          is_disabled = True
        
        # For Clutch.co specifically, check the clickability of the button
        try:
          is_clickable = await page.evaluate("""(button) => {
            const style = window.getComputedStyle(button);
            const isVisible = style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
            return isVisible && !button.disabled;
          }""", next_button)
          
          if not is_clickable:
            is_disabled = True
        except:
          pass
        
        if is_visible and not is_disabled:
          logging.info(f"✅ Found next page button with selector: {selector}")
          return next_button
        elif is_visible and is_disabled:
          logging.info(f"🛑 Found visible but disabled next button with selector: {selector} - Reached last page")
          return None  # Return None to indicate end of pagination
    
    # If no selector worked, try a more generic approach for Clutch.co
    # Look for pagination elements and find the active one to determine if there's a next page
    try:
      # Get all pagination items
      pagination_items = await page.query_selector_all('.pagination .page-item, .pager-item')
      if pagination_items and len(pagination_items) > 0:
        # Find the active item
        active_index = -1
        for i, item in enumerate(pagination_items):
          class_attr = await item.get_attribute('class') or ""
          if "active" in class_attr or "pager-current" in class_attr:
            active_index = i
            break
        
        # Check if there are more pages after the active one
        if active_index >= 0 and active_index < len(pagination_items) - 1:
          next_item = pagination_items[active_index + 1]
          # Skip if this is just a "next" button that might be disabled
          next_item_text = await next_item.text_content()
          if next_item_text and ("next" in next_item_text.lower() or "»" in next_item_text):
            # Check if it's disabled
            next_class = await next_item.get_attribute('class') or ""
            if "disabled" in next_class:
              logging.info("❌ Next pagination item is disabled")
              return None
              
          next_link = await next_item.query_selector('a')
          if next_link:
            # Check if the link is disabled
            is_disabled = False
            try:
              parent_class = await next_item.get_attribute('class') or ""
              if "disabled" in parent_class:
                is_disabled = True
            except:
              pass
              
            if not is_disabled:
              logging.info("✅ Found next page button using pagination analysis")
              return next_link
    except Exception as e:
      logging.error(f"Error during pagination analysis: {e}")
    
    # Last attempt: check if we're on the last page by analyzing the URL and page numbers
    try:
      # Check URL for page parameter
      current_url = page.url
      current_page_match = re.search(r'page=(\d+)', current_url)
      
      if current_page_match:
        current_page_num = int(current_page_match.group(1))
        
        # Try to find the highest page number in pagination
        max_page_num = current_page_num  # Default to current
        page_links = await page.query_selector_all('.pagination a, .pager a')
        
        for link in page_links:
          try:
            href = await link.get_attribute('href') or ""
            page_match = re.search(r'page=(\d+)', href)
            if page_match:
              page_num = int(page_match.group(1))
              max_page_num = max(max_page_num, page_num)
          except:
            pass
        
        if current_page_num >= max_page_num:
          logging.info(f"🛑 Current page {current_page_num} appears to be the last page based on URL analysis")
          return None
    except Exception as e:
      logging.warning(f"Error during URL pagination analysis: {e}")
      
    # If we get here, no next button was found
    logging.info("❌ No next page button found")
    return None
  except Exception as e:
    logging.error(f"Error checking for next page: {e}")
    return None


async def extract_current_page_number(page):
  """Extract the current page number from pagination elements, optimized for Clutch.co"""
  try:
    # Try different selectors to find current page - Clutch.co specific first
    selectors = [
      '.pagination .page-item.active a',  # Clutch.co uses this
      '.pagination .page-item.active',
      '.pagination li.active',
      'a.page-link.active',
      '.pagination .selected',
      '.pagination__current',
      'li.active span'
    ]
    
    for selector in selectors:
      element = await page.query_selector(selector)
      if element:
        # Try to get text content
        text = await element.text_content()
        if text:
          # Extract numeric value
          match = re.search(r'\d+', text)
          if match:
            return int(match.group())
    
    # Special case for Clutch.co where active page might not have a number
    # Find all page items and determine which one is active
    pagination_items = await page.query_selector_all('.pagination .page-item')
    if pagination_items:
      for i, item in enumerate(pagination_items):
        class_attr = await item.get_attribute('class') or ""
        if "active" in class_attr or "pager-current" in class_attr:
          # The active item index might correspond to page number
          # But need to account for potential "previous" button at the beginning
          text = await item.text_content()
          if text:
            match = re.search(r'\d+', text)
            if match:
              return int(match.group())
          # If we couldn't extract number from text, try using index
          # Check if the first item is "previous" or has an arrow
          first_item = pagination_items[0]
          first_text = await first_item.text_content()
          if first_text and ("prev" in first_text.lower() or "<" in first_text):
            return i  # 0-indexed with prev button would be page 1, etc.
          else:
            return i + 1  # 0-indexed without prev button would be page 0+1, etc.
    
    # Fallback: try to extract from URL
    url = page.url
    match = re.search(r'page=(\d+)', url)
    if match:
      return int(match.group(1))
    
    # On Clutch.co, if there's no page parameter, we're on page 1
    return 1
  except Exception as e:
    logging.warning(f"Error extracting page number: {e}")
    return 1


async def navigate_to_next_page(page):
  """Enhanced navigation to the next page specifically optimized for Clutch.co"""
  from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
  try:
    # Find next button using our improved detector
    next_button = await has_next_page(page)
    
    if next_button:
      # Get the current URL and page number before clicking
      current_url = page.url
      current_page_num = await extract_current_page_number(page)
      logging.info(f"⏭️ Navigating from page {current_page_num} at URL: {current_url}")
      
      # For Clutch.co, extract the href attribute directly
      next_url = None
      try:
        href = await next_button.get_attribute('href')
        if href:
          # Check if it's a full URL or relative path
          if href.startswith('http'):
            next_url = href
          else:
            # Construct full URL from relative path, preserving query params
            parsed_url = urlparse(current_url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            # If href has its own query, use it, else preserve original query
            if '?' in href:
              next_url = urljoin(base_url, href)
            else:
              # preserve all query params
              next_url = urljoin(base_url, href)
              if parsed_url.query:
                next_url += ('&' if '?' in next_url else '?') + parsed_url.query
      except Exception as e:
        logging.warning(f"Error getting href from next button: {e}")
      
      # Scroll the button into view and wait briefly
      await next_button.scroll_into_view_if_needed()
      await page.wait_for_timeout(1000)
      
      # First try direct navigation if we have a URL
      if next_url:
        try:
          logging.info(f"🔄 Navigating directly to next page URL: {next_url}")
          await page.goto(next_url, wait_until="domcontentloaded")
          # Wait for the page to load
          await page.wait_for_timeout(3000)
          
          # Verify we actually navigated to a valid page
          if await is_valid_results_page(page):
            new_page_num = await extract_current_page_number(page)
            if new_page_num > current_page_num:
              logging.info(f"✅ Successfully navigated to page {new_page_num} via direct URL")
              return True
            else:
              logging.warning(f"⚠️ Navigation may have failed: Expected page > {current_page_num}, got {new_page_num}")
              # We'll still continue and try clicking
          else:
            logging.warning("⚠️ Direct navigation led to invalid page, trying alternate methods")
        except Exception as e:
          logging.warning(f"⚠️ Direct navigation failed: {e}")
          # Fallback to clicking the button
      
      # Try clicking with retry logic
      max_click_attempts = 3
      for attempt in range(max_click_attempts):
        try:
          # Try to click in different ways
          if attempt == 0:
            logging.info("🖱️ Attempting standard click on next button")
            await next_button.click()
          elif attempt == 1:
            # Try JavaScript click as alternative
            logging.info("🖱️ Attempting JavaScript click on next button")
            await page.evaluate("(button) => button.click()", next_button)
          else:
            # Last resort: construct and navigate to next page URL manually
            logging.info("🔄 Constructing next page URL manually (preserving query params)")
            parsed_url = urlparse(current_url)
            query = parse_qs(parsed_url.query)
            current_page_num = int(query.get('page', [1])[0]) if 'page' in query else 1
            query['page'] = [str(current_page_num + 1)]
            next_query = urlencode(query, doseq=True)
            next_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', next_query, ''))
            logging.info(f"🔄 Attempting navigation to constructed URL: {next_url}")
            await page.goto(next_url, wait_until="domcontentloaded")
          break
        except Exception as click_error:
          if attempt < max_click_attempts - 1:
            logging.warning(f"⚠️ Click attempt {attempt+1} failed, retrying: {click_error}")
            await page.wait_for_timeout(1000)
          else:
            raise
      # Wait for navigation to complete with progressive waits
      try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
        await page.wait_for_timeout(2000)
        
        # Wait for network to be idle
        await page.wait_for_load_state("networkidle", timeout=10000)
        
        # Wait for content to be visible - specific for Clutch.co
        await page.wait_for_selector('.provider-info, .providers-directory, .listing-companies, .page-item', timeout=10000)
      except Exception as e:
        # If timeout, still continue as the page might have loaded enough
        logging.warning(f"⚠️ Wait timeout, but continuing: {e}")
      
      # Verify we actually changed pages
      new_url = page.url
      new_page_num = await extract_current_page_number(page)
      
      # Add content hash verification to confirm we have different content
      content_hash = await page.evaluate("""() => {
        // Get the first few business items on the page
        const items = Array.from(document.querySelectorAll('.provider-info, .listing-companies a[href*="r.clutch.co/redirect"]')).slice(0, 5);
        return items.map(el => el.textContent || el.href || '').join('|');
      }""")
      
      if hasattr(page, 'content_hash_previous_page') and content_hash == page.content_hash_previous_page:
        logging.warning("⚠️ Page content appears identical to previous page despite URL change")
        
      # Store current hash for next comparison
      page.content_hash_previous_page = content_hash
      
      # Verify we're on a valid results page
      is_valid_page = await is_valid_results_page(page)
      
      # Check if this is a unique page we haven't seen before
      is_unique_content = await verify_unique_page_content(page, new_page_num)
      if not is_valid_page:
        logging.warning("⚠️ Navigation led to an invalid page (possibly a 'No Results' or error page)")
        return False
      
      if not is_unique_content:
        logging.warning("⚠️ Navigation led to a duplicate page we've seen before")
        # We still return True because we did navigate, just to a duplicate page
        # The main scraping loop will handle this with the consecutive_duplicate_pages logic
      
      if new_url == current_url and new_page_num == current_page_num:
        logging.warning("⚠️ URL and page number did not change after navigation attempt")
        
        # Allow continuation for Clutch.co when URL contains our domain
        if "clutch.co" in current_url:
          logging.info("🔄 This is a Clutch.co page - checking for valid content despite similar URLs")
          
          # Check if the page has business links regardless of URL similarity
          try:
            sample_links = await page.query_selector_all('a[href*="r.clutch.co/redirect"]')
            if sample_links and len(sample_links) > 0:
              logging.info(f"✅ Found {len(sample_links)} business links on page despite URL similarity - continuing")
              return True
          except Exception as e:
            logging.warning(f"⚠️ Error checking page content after navigation: {e}")
            
        # Try JavaScript navigation as fallback specifically for Clutch.co
        if current_page_num:
          next_page_num = current_page_num + 1
          
          # Try to extract base URL from current URL for Clutch.co format
          base_url = re.sub(r'\?.*$', '', current_url) # Remove query string
          
          # Construct next page URL in Clutch.co format
          next_url = f"{base_url}?page={next_page_num}"
          logging.info(f"🔄 Final attempt: JavaScript navigation to: {next_url}")
          
          # Use JavaScript navigation
          await page.evaluate(f"""() => {{
            window.location.href = "{next_url}";
          }}""")
          
          await page.wait_for_timeout(5000)
          
          # Check if we actually navigated
          final_url = page.url
          if final_url != current_url:
            logging.info(f"✅ Successfully navigated to: {final_url}")
            return await is_valid_results_page(page)
          else:
            logging.error("❌ All navigation attempts failed")
            return False
      # Give the page time to stabilize and simulate human behavior
      await page.wait_for_timeout(3000)
      await simulate_human_behavior(page)
      
      # Check if we're on a page for Clutch.co after navigation
      try:
        await page.wait_for_selector('.provider-info, .providers-directory, .listing-companies, .page-item', timeout=5000)
        logging.info(f"✅ Successfully navigated to next page: {page.url}")
        return True
      except:
        logging.warning("⚠️ Navigation succeeded but page content may be different than expected")
        return await is_valid_results_page(page)
    
    return False
  except Exception as e:
    logging.error(f"Error navigating to next page: {e}")
    return False


async def scrape_all_pages(page, start_url, writer, csvfile, unique_businesses):
  """Scrape all pages from a given start URL, optimized for Clutch.co"""
  total_entries = 0
  total_businesses_found = 0
  page_num = 1
  max_pages = 100  # Increased for Clutch.co which can have many pages
  consecutive_empty_pages = 0
  max_empty_pages = 3  # Stop if we find 3 empty pages in a row
  consecutive_navigation_failures = 0
  max_navigation_failures = 2  # Stop if we can't navigate properly multiple times
  consecutive_duplicate_pages = 0
  # No longer using max_duplicate_pages as a stopping condition
  
  # Navigate to start URL
  try:
    logging.info(f"🌐 Navigating to start URL: {start_url}")
    await page.goto(start_url, wait_until='domcontentloaded', timeout=30000)
    await page.wait_for_timeout(5000)
    
    # Check if page loaded properly by looking for common elements on Clutch.co
    content_check = await page.query_selector('.provider-info, .providers-directory, .listing-companies, .page-item, a[href*="r.clutch.co/redirect"]')
    if not content_check:
      logging.warning(f"⚠️ Page may not have loaded properly: {start_url}")
      # Try taking a screenshot to diagnose issues
      try:
        screenshot_path = f"logs/failed_load_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=screenshot_path)
        logging.info(f"📸 Saved failure screenshot to {screenshot_path}")
      except:
        pass
      
      # Try reloading
      logging.info("🔄 Attempting to reload the page")
      await page.reload(wait_until='domcontentloaded')
      await page.wait_for_timeout(5000)
  except Exception as e:
    logging.error(f"❌ Error loading start URL: {e}")
    await page.reload()
    await page.wait_for_timeout(5000)
  
  # Check for Cloudflare
  if await is_cloudflare_active(page):
    logging.info("⚠️ Cloudflare challenge detected, skipping this URL")
    return total_entries, total_businesses_found
  

  # Enhanced human simulation
  await simulate_human_behavior(page)
  
  # Loop through all pages
  while page_num <= max_pages:
    current_url = page.url
    logging.info(f"📄 Processing page {page_num} of {start_url} (Current URL: {current_url})")
    
    # Verify this is a valid results page before proceeding
    if not await is_valid_results_page(page):
      logging.warning(f"⚠️ Page {page_num} appears to be invalid (no results or error page)")
      consecutive_navigation_failures += 1
      if consecutive_navigation_failures >= max_navigation_failures:
        logging.info(f"🛑 {max_navigation_failures} consecutive navigation failures, stopping pagination")
        break
      else:
        # Try to navigate to the next page anyway
        page_num += 1
        continue
    else:
      # Reset counter if we found a valid page
      consecutive_navigation_failures = 0
    
    # For Clutch.co, ensure we capture all lazy-loaded content
    for _ in range(3):  # Multiple scrolls to ensure all content loads
      await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
      await page.wait_for_timeout(1000)
      await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
      await page.wait_for_timeout(2000)
    
    # Get business links from the current page
    business_links = await get_business_links_from_page(page)
    
    # If no links found, track consecutive empty pages
    if not business_links:
      consecutive_empty_pages += 1
      logging.warning(f"🔍 No business links found on page {page_num}, empty count: {consecutive_empty_pages}/{max_empty_pages}")
      
      if consecutive_empty_pages >= max_empty_pages:
        logging.info(f"🛑 {max_empty_pages} consecutive empty pages, stopping pagination")
        break
      
      # For Clutch.co, only look for direct "Visit Website" links
      logging.info("🔍 Trying Clutch.co direct 'Visit Website' links")
      try:
        # Try to find all "Visit Website" links which are common on Clutch.co
        website_links = await page.query_selector_all('a[href*="r.clutch.co/redirect"]')
        if website_links and len(website_links) > 0:
          logging.info(f"✅ Found {len(website_links)} Clutch.co redirect links")
          business_links = website_links
          consecutive_empty_pages = 0
        else:
          # Try any link that says "Visit Website" or similar
          visit_website_links = await page.query_selector_all('a:has-text("Visit Website"), a[title*="Visit Website"], a.visit-website')
          if visit_website_links and len(visit_website_links) > 0:
            logging.info(f"✅ Found {len(visit_website_links)} generic 'Visit Website' links")
            business_links = visit_website_links
            consecutive_empty_pages = 0
      except Exception as e:
        logging.error(f"❌ Error finding 'Visit Website' links: {e}")
      
      # If still no links, try one more time with a page reload
      if not business_links:
        logging.info(f"🔍 Still no business links found after Clutch.co selectors on page {page_num}")
        
        # Try one more time with a page reload
        await page.reload()
        await page.wait_for_timeout(5000)
        await simulate_human_behavior(page)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(3000)
        business_links = await get_business_links_from_page(page)
        
        if not business_links:
          logging.info(f"🔍 Still no business links after reload on page {page_num}")
          # Continue to next page instead of breaking immediately
    else:
      # Reset consecutive empty pages counter if we found links
      consecutive_empty_pages = 0
    
    # Process the business links
    original_unique_count = len(unique_businesses)
    entries = await process_business_links(page, business_links, writer, csvfile, unique_businesses)
    new_unique_count = len(unique_businesses)
    new_businesses_found = new_unique_count - original_unique_count
    
    # OPTIMIZED: Simplified duplicate tracking - no longer stop or warn based on duplicates
    # Featured businesses will naturally appear as duplicates, this is expected behavior
    if new_businesses_found > 0:
      logging.info(f"📊 Page {page_num}: Added {new_businesses_found} NEW businesses (processed {len(business_links)} total links)")
      consecutive_duplicate_pages = 0  # Reset any duplicate counter
    else:
      logging.info(f"📊 Page {page_num}: No new businesses added (all {len(business_links)} were duplicates/featured - this is normal)")
      # Don't increment consecutive_duplicate_pages - just continue processing
    
    total_entries += entries
    total_businesses_found += len(business_links)
    
    logging.info(f"✅ Page {page_num}: Completed processing {len(business_links)} businesses")
    
    # Check if there's a next page with our improved function
    next_button = await has_next_page(page)
    if not next_button:
      logging.info(f"🏁 Reached the last page ({page_num}) for {start_url} - Next button is disabled or doesn't exist")
      
      # Take a screenshot of the last page for verification
      try:
        last_page_screenshot = f"logs/last_page_{page_num}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=last_page_screenshot)
        logging.info(f"📸 Saved last page screenshot to {last_page_screenshot}")
      except:
        pass
        
      break  # Only stop pagination when Next button is disabled or doesn't exist
    
    # Navigate to the next page with our improved function
    navigation_success = await navigate_to_next_page(page)
    if not navigation_success:
      logging.warning(f"⚠️ Failed to navigate to next page after page {page_num}")
      consecutive_navigation_failures += 1
      
      if consecutive_navigation_failures >= max_navigation_failures:
        logging.info(f"🛑 {max_navigation_failures} consecutive navigation failures, stopping pagination")
        
        # Even after navigation failures, check if we can find a disabled next button
        # to confirm we've actually reached the end of pagination
        try:
          disabled_next = await page.query_selector('.pagination .page-item.next.disabled, li.pager-next.disabled')
          if disabled_next and await disabled_next.is_visible():
            logging.info("✅ Confirmed we're at the last page (found disabled next button)")
          else:
            logging.warning("⚠️ Navigation failed but no disabled next button found - may have missed some pages")
        except:
          pass
          
        break
        
      # For Clutch.co, try a direct URL approach as last resort
      current_page_num = await extract_current_page_number(page)
      next_page_num = current_page_num + 1
      
      # Try to construct next page URL in Clutch.co format, preserving query params
      try:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed_url = urlparse(current_url)
        query = parse_qs(parsed_url.query)
        query['page'] = [str(next_page_num)]
        next_query = urlencode(query, doseq=True)
        next_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', next_query, ''))
        logging.info(f"🔄 Attempting direct navigation to Clutch.co format URL: {next_url}")
        await page.goto(next_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)
        await simulate_human_behavior(page)
        # Verify navigation worked
        new_page_num = await extract_current_page_number(page)
        if new_page_num != next_page_num:
          logging.warning(f"⚠️ Direct navigation did not work: Expected page {next_page_num}, got {new_page_num}")
          break
      except Exception as e:
        logging.error(f"❌ Manual navigation also failed: {e}")
        break
    else:
      # Reset the counter if navigation was successful
      consecutive_navigation_failures = 0
    
    page_num += 1
    
    # Give the server a brief rest between pages (good practice)
    await page.wait_for_timeout(random.randint(2000, 4000))
    
  logging.info(f"📊 Finished scraping {start_url}: {total_businesses_found} businesses found across {page_num} pages")
  return total_entries, total_businesses_found


async def verify_unique_page_content(page, current_page_num):
  """Verify that the page content is unique compared to previously visited pages
  Returns True if the content is unique, False if duplicate"""
  try:
    # Sample business links from the page for fingerprinting - try multiple selectors
    sample_links = []
    
    # Try Clutch.co redirect links first
    try:
      clutch_links = await page.evaluate("""() => {
        const links = Array.from(document.querySelectorAll('a[href*="r.clutch.co/redirect"]')).slice(0, 15);
        return links.map(a => a.href || '');
      }""")
      if clutch_links and len(clutch_links) > 0:
        sample_links = clutch_links
    except Exception:
      pass
      
    # If no Clutch redirect links found, try business names
    if not sample_links:
      try:
        business_names = await page.evaluate("""() => {
          const elements = Array.from(document.querySelectorAll('.provider-info h3, .provider-name, .company-name, .listing-item h3')).slice(0, 15);
          return elements.map(el => el.textContent.trim());
        }""")
        if business_names and len(business_names) > 0:
          sample_links = business_names
      except Exception:
        pass
    
    # If still nothing, try any external links
    if not sample_links:
      try:
        external_links = await page.evaluate("""() => {
          const links = Array.from(document.querySelectorAll('a[href^="http"]')).slice(0, 20);
          return links.map(a => a.href || '').filter(href => 
            !href.includes('clutch.co/directories') && 
            !href.includes('clutch.co/profile') &&
            !href.includes('facebook.com') &&
            !href.includes('twitter.com') &&
            !href.includes('linkedin.com')
          );
        }""")
        if external_links and len(external_links) > 0:
          sample_links = external_links
      except Exception:
        pass
    
    # If we have enough data, create a fingerprint
    if sample_links and len(sample_links) >= 3:
      fingerprint = "|".join(sorted(sample_links))
      
      # Initialize page fingerprints if not exists
      if not hasattr(page, 'previous_page_fingerprints'):
        page.previous_page_fingerprints = set()
      
      # Check if we've seen this content before
      if fingerprint in page.previous_page_fingerprints:
        duplicate_percent = round((len(sample_links) / len(sample_links)) * 100, 1)
        logging.warning(f"⚠️ Page {current_page_num} content appears to be a duplicate of a previous page ({duplicate_percent}% match)")
        return False
      
      # Store fingerprint for future comparison
      page.previous_page_fingerprints.add(fingerprint)
      logging.info(f"✅ Page {current_page_num} content is unique (based on {len(sample_links)} sample links/items)")
    else:
      logging.warning(f"⚠️ Not enough content found on page {current_page_num} to verify uniqueness")
      
    return True  # Continue if we can't determine uniqueness
  except Exception as e:
    logging.error(f"Error verifying page uniqueness: {e}")
    return True  # Continue on error


async def process_single_business(i, link, page_context, writer, csvfile, unique_businesses, semaphore):
  """Process a single business link with semaphore control for concurrency"""
  business_page = None
  entries = 0
  
  try:
    href = await link.get_attribute('href')
    if not href:
      return 0
    
    # Double-check to ensure we're not accessing Clutch profiles
    if "clutch.co/profile" in href:
      logging.info(f"🚫 Skipping profile link: {href}")
      return 0
      
    # Create new page for the business
    business_page = await page_context.new_page()
    max_retries = 2
    retry_count = 0
    
    while retry_count <= max_retries:
      try:
        # Increase timeout for navigation if this is a retry
        timeout = 30000 if retry_count == 0 else 45000
        
        # Log navigation attempt
        logging.info(f"🔗 Navigating to business {i+1}: {href}" + 
                    (f" (Retry {retry_count})" if retry_count > 0 else ""))
        
        await business_page.goto(href, wait_until='domcontentloaded', timeout=timeout)
        await business_page.wait_for_timeout(2000)
        
        # Check for Cloudflare
        if await is_cloudflare_active(business_page):
          logging.info(f"⚠️ Cloudflare detected for business {i+1}, skipping...")
          break
          
        # Light human simulation
        await simulate_human_behavior(business_page)

        # Scrape business info
        actual_url = business_page.url
        entries = await scrape_business_info(business_page, actual_url, writer, csvfile, unique_businesses)
        
        # If we get here, the scraping was successful
        break
        
      except Exception as nav_error:
        if "Timeout" in str(nav_error) and retry_count < max_retries:
          retry_count += 1
          logging.warning(f"⚠️ Timeout navigating to business {i+1}, retrying ({retry_count}/{max_retries})...")
          
          # Wait before retrying
          await asyncio.sleep(2)
        else:
          logging.error(f"❌ Error navigating to business {i+1}: {nav_error}")
          break

  except Exception as e:
    logging.error(f"❌ Error creating page for business {i+1}: {e}")
  
  finally:
    if business_page:
      try:
        await business_page.close()
      except:
        pass
    # Release the semaphore when done
    semaphore.release()
      
  return entries


async def process_business_links(page, links, writer, csvfile, unique_businesses):
  """Process all business links from current page using parallel execution"""
  if not links:
    return 0
    
  # OPTIMIZED: Don't pre-filter duplicates - process all and handle duplicates in CSV writing
  # This prevents skipping entire pages due to featured businesses
  hrefs = []
  processed_urls = set()
  skipped_profile_count = 0
  
  for link in links:
    href = await link.get_attribute('href')
    # Only skip profile links and already processed URLs in this batch
    if href and href not in processed_urls:
      if "clutch.co/profile" in href:
        skipped_profile_count += 1
        continue  # Skip this link
        
      # DON'T skip duplicates here - let them be processed and handled in CSV writing
      # This ensures we don't skip entire pages due to featured businesses
      processed_urls.add(href)
      hrefs.append((link, href))
  
  total_links = len(hrefs)
  if skipped_profile_count > 0:
    logging.info(f"🔍 Skipped {skipped_profile_count} profile links")
  
  # Note: We now process ALL business links, duplicates will be handled during CSV writing
  logging.info(f"🚀 Processing {total_links} business links in parallel (duplicates handled during save)...")

  # Create a semaphore to limit concurrent executions
  semaphore = asyncio.Semaphore(max_concurrent)
  
  # Create tasks for each business link
  tasks = []
  for i, (link, _) in enumerate(hrefs):
    # Acquire semaphore (will wait if max_concurrent is reached)
    await semaphore.acquire()
    
    # Create task for this business
    task = asyncio.create_task(
      process_single_business(i, link, page.context, writer, csvfile, unique_businesses, semaphore)
    )
    tasks.append(task)
    
    # Log progress periodically
    if (i + 1) % 10 == 0 or i == total_links - 1:
      logging.info(f"⏳ Created tasks for {i + 1}/{total_links} businesses")
  
  # Wait for all tasks to complete and collect results
  results = await asyncio.gather(*tasks)
  total_entries = sum(results)
  
  duplicate_count = total_links - total_entries
  if duplicate_count > 0:
    logging.info(f"📊 Processed {total_links} links: {total_entries} new entries, {duplicate_count} duplicates filtered")
  else:
    logging.info(f"✅ Completed processing {total_links} businesses with {total_entries} entries added")
  
  return total_entries


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
      logging.warning("⚠️ No URLs found in scrape_urls.txt. Please add URLs to the file.")
      urls = []
    
    return urls
  except FileNotFoundError:
    logging.warning("⚠️ scrape_urls.txt not found. Please create this file with URLs to scrape.")
    return []
  except Exception as e:
    logging.error(f"❌ Error reading scrape_urls.txt: {e}")
    return []


async def is_valid_results_page(page):
  """Check if the current page contains valid search results or is an error/empty page"""
  try:
    # First, check for common error messages or "No Results Found" indicators
    error_selectors = [
      '.no-results-found', 
      '.error-page', 
      '.empty-results',
      'text="No results found"',
      'text="No matching results"',
      'text="Sorry, no companies match your filters"',
      '.search-no-results'
    ]
    
    for selector in error_selectors:
      try:
        error_element = await page.query_selector(selector)
        if error_element and await error_element.is_visible():
          logging.warning(f"⚠️ Found error/no results indicator: {selector}")
          return False
      except:
        pass
    
    # Check for presence of business listings or pagination
    valid_content_selectors = [
      '.provider-info', 
      '.providers-directory', 
      '.listing-companies',
      '.pagination',
      'a[href*="r.clutch.co/redirect"]',
      '.listing-item',
      '.provider-row'
    ]
    
    for selector in valid_content_selectors:
      try:
        content_elements = await page.query_selector_all(selector)
        if content_elements and len(content_elements) > 0:
          # Check if at least one element is visible
          for element in content_elements:
            if await element.is_visible():
              return True
      except:
        pass
    
    # If we get here and haven't found valid content, do one more check:
    # Try to find any external links that might be business websites
    try:
      external_links = await page.evaluate('''() => {
        const links = Array.from(document.querySelectorAll('a[href^="http"]'));
        const externalLinks = links.filter(a => 
          !a.href.includes(window.location.hostname) && 
          !a.href.includes('facebook.com') && 
          !a.href.includes('twitter.com') && 
          !a.href.includes('linkedin.com') &&
          !a.href.includes('google.com') &&
          !a.href.includes('clutch.co/directories') &&
          !a.href.includes('clutch.co/about-us') &&
          !a.href.includes('clutch.co/methodology')
        );
        return externalLinks.length;
      }''')
      
      if external_links > 5:  # If we found a good number of external links, it's probably a valid page
        return True
    except:
      pass
    
    # If we've exhausted all checks and found nothing, it's probably not a valid results page
    logging.warning("⚠️ Page doesn't appear to contain valid search results")
    return False
    
  except Exception as e:
    logging.error(f"Error checking if page is valid: {e}")
    # Default to True in case of error, to avoid breaking the scraping process
    return True


def cleanup_csv_duplicates(csv_filename):
  """Remove duplicate entries from CSV file based on business_name"""
  try:
    # Read the CSV file
    with open(csv_filename, 'r', newline='', encoding='utf-8') as file:
      reader = csv.DictReader(file)
      rows = list(reader)
    
    # Remove duplicates based on business_name (case-insensitive)
    seen_businesses = set()
    unique_rows = []
    duplicates_removed = 0
    
    for row in rows:
      business_name_lower = row['business_name'].lower().strip()
      if business_name_lower not in seen_businesses:
        seen_businesses.add(business_name_lower)
        unique_rows.append(row)
      else:
        duplicates_removed += 1
    
    # Write the cleaned data back to the file
    with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
      if unique_rows:
        fieldnames = unique_rows[0].keys()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unique_rows)
    
    logging.info(f"🧹 CSV cleanup: Removed {duplicates_removed} duplicate entries")
    logging.info(f"📄 Final CSV contains {len(unique_rows)} unique businesses")
    
    return len(unique_rows), duplicates_removed
    
  except Exception as e:
    logging.error(f"❌ Error cleaning CSV file: {e}")
    return 0, 0


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
      # Read URLs from file
      base_urls = await read_base_urls()
      
      # Check if we have any URLs to process
      if not base_urls:
        logging.error("❌ No URLs to scrape. Please add URLs to scrape_urls.txt file.")
        return
      
      logging.info(f"📋 Found {len(base_urls)} URLs to scrape: {', '.join(base_urls)}")

      # Process businesses and save to CSV
      unique_businesses = set()
      total_entries = 0
      total_businesses_found = 0
      csvfile = None
      
      with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['website_url', 'business_name', 'email', 'phone']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        logging.info(f"📝 Created CSV file: {csv_filename}")

        for start_url in base_urls:
          try:
            logging.info(f"🌐 Starting to scrape URL with pagination: {start_url}")
            
            # Use the new function to scrape all pages
            entries, businesses_found = await scrape_all_pages(page, start_url, writer, csvfile, unique_businesses)
            
            # Update totals
            total_entries += entries
            total_businesses_found += businesses_found
            
            logging.info(f"✅ Finished URL: {start_url} - Found {businesses_found} businesses, added {entries} entries")
            
          except Exception as e:
            logging.error(f"Error processing URL {start_url}: {e}")

      logging.info(f"🎉 Scraping completed! Total entries: {total_entries}")

      # Cleanup duplicates from the final CSV
      try:
        logging.info("🧹 Cleaning up duplicates in the final CSV file...")
        total_unique, total_duplicates = cleanup_csv_duplicates(csv_filename)
        logging.info(f"📊 CSV Cleanup completed: {total_unique} unique entries, {total_duplicates} duplicates removed")
      except Exception as e:
        logging.error(f"Error during CSV cleanup: {e}")

    except Exception as e:
      logging.error(f"Error in main processing: {e}")

    finally:
      await page.close()
      await context.close()
      await browser.close()

  logging.info("✅ Browser closed, exiting program.")


# Run the scraper
asyncio.run(main())
