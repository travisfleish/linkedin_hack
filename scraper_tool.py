import os
import time
import random
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import openai
import platform

# Load environment variables
load_dotenv()


class LinkedInScraper:
    def __init__(self, linkedin_email, linkedin_password):
        self.linkedin_email = linkedin_email
        self.linkedin_password = linkedin_password

        # Configure Chrome options with anti-detection measures
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--start-maximized")

        # Add random user agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0"
        ]
        chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

        # Create WebDriver with the options
        self.driver = webdriver.Chrome(options=chrome_options)

        # Add this after creating the driver
        # This makes selenium stealth, harder to detect
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
        })

        self.wait = WebDriverWait(self.driver, 20)  # Increased timeout

        # Start with login
        self._login()

    def _login(self):
        """Log in to LinkedIn"""
        try:
            self.driver.get("https://www.linkedin.com/login")
            print("Opening LinkedIn login page...")

            # Wait for the login page to load
            self.wait.until(EC.presence_of_element_located((By.ID, "username")))

            # Accept cookies if prompted
            try:
                print("Checking for cookie consent dialog...")
                cookie_button = self.driver.find_element(By.CSS_SELECTOR, "button[action-type='ACCEPT']")
                print("Cookie dialog found. Accepting cookies...")
                cookie_button.click()
                time.sleep(1)  # Short delay after clicking
            except:
                print("No cookie dialog found, continuing...")

            print(
                f"Entering email: {self.linkedin_email[:3]}{'*' * (len(self.linkedin_email) - 6)}{self.linkedin_email[-3:]}")

            # Enter credentials
            username_input = self.driver.find_element(By.ID, "username")
            password_input = self.driver.find_element(By.ID, "password")

            username_input.send_keys(self.linkedin_email)
            password_input.send_keys(self.linkedin_password)

            # Click the login button
            print("Clicking login button...")
            login_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            login_button.click()

            # Wait for login to complete
            print("Waiting for login to complete...")
            self.wait.until(EC.url_contains("feed"))
            print("✅ Successfully logged in to LinkedIn")

        except TimeoutException:
            print("⚠️ Login failed - timeout occurred waiting for the feed page")
            raise
        except Exception as e:
            print(f"❌ Login failed: {str(e)}")
            raise

    def scrape_profile(self, profile_url):
        """Scrape a LinkedIn profile and extract relevant information"""
        try:
            # Add random delay to avoid rate limiting
            delay = random.uniform(3, 7)
            print(f"Adding delay of {delay:.2f} seconds before visiting profile...")
            time.sleep(delay)

            # Navigate to profile
            print(f"Navigating to: {profile_url}")
            self.driver.get(profile_url)

            # Check for security challenges
            if "checkpoint" in self.driver.current_url or "security" in self.driver.current_url:
                print("⚠️ LinkedIn security check detected. Please solve it manually.")
                input("Press Enter after resolving the security challenge...")

                # Reload the page after challenge is solved
                print("Reloading profile page...")
                self.driver.get(profile_url)

            # Increase wait time for slow connections
            self.wait = WebDriverWait(self.driver, 20)  # Increase from 10 to 20 seconds

            # Wait for profile to load - using a more reliable selector
            print("Waiting for profile to load...")

            # Try multiple possible selectors for the profile section
            possible_selectors = [
                ".pv-top-card",
                ".profile-background-image",
                ".ph5",
                "h1",
                ".artdeco-card",
                ".pv-profile-section"
            ]

            profile_loaded = False
            for selector in possible_selectors:
                try:
                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    profile_loaded = True
                    print(f"Profile loaded successfully using selector: {selector}")
                    break
                except:
                    continue

            if not profile_loaded:
                print("⚠️ Couldn't detect profile load with standard selectors. Taking screenshot...")
                # Take a screenshot for debugging
                timestamp = int(time.time())
                screenshot_path = f"profile_debug_{timestamp}.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"Screenshot saved to {screenshot_path}")

                # Continue anyway and try to extract whatever we can
                time.sleep(5)  # Additional wait
                print("Attempting to extract data anyway...")

            # Extract profile data
            profile_data = {}

            # Name - try multiple selectors with broader approach
            name_selectors = [
                "h1.text-heading-xlarge",
                "h1.inline.t-24.t-black.t-normal.break-words",
                ".pv-top-card--list li:first-child",
                "h1.text-heading-large",
                ".text-heading-xlarge",
                "h1",  # Most generic - try this last
                ".artdeco-entity-lockup__title",
                ".pv-top-card h1"
            ]

            print("Extracting name...")
            for selector in name_selectors:
                try:
                    name_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    profile_data["name"] = name_element.text.strip()
                    print(f"Found name: {profile_data['name']}")
                    break
                except:
                    continue

            # If all selectors fail, try to extract name from page title
            if "name" not in profile_data or not profile_data["name"]:
                try:
                    page_title = self.driver.title
                    # LinkedIn titles are usually in format: "Name - Title | LinkedIn"
                    if " - " in page_title and " | LinkedIn" in page_title:
                        profile_data["name"] = page_title.split(" - ")[0].strip()
                        print(f"Extracted name from page title: {profile_data['name']}")
                    elif " | LinkedIn" in page_title:
                        # Sometimes it's just "Name | LinkedIn"
                        profile_data["name"] = page_title.split(" | LinkedIn")[0].strip()
                        print(f"Extracted name from page title: {profile_data['name']}")
                except:
                    pass

            if "name" not in profile_data or not profile_data["name"]:
                profile_data["name"] = ""
                print("⚠️ Could not find name element")

            # Title - try multiple selectors
            title_selectors = [
                ".text-body-medium.break-words",
                ".pv-top-card--list li:nth-child(2)",
                ".ph5 div.display-flex.flex-wrap.align-items-center div span",
                "[data-field='headline']",
                ".pv-top-card .text-body-medium",
                ".pv-top-card-section__headline",
                ".text-body-medium",
                ".pvs-header__subtitle"
            ]

            print("Extracting title...")
            for selector in title_selectors:
                try:
                    title_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    profile_data["title"] = title_element.text.strip()
                    print(f"Found title: {profile_data['title']}")
                    break
                except:
                    continue

            # If all selectors fail, try to extract title from page title
            if "title" not in profile_data or not profile_data["title"]:
                try:
                    page_title = self.driver.title
                    # LinkedIn titles are usually in format: "Name - Title | LinkedIn"
                    if " - " in page_title and " | LinkedIn" in page_title:
                        profile_data["title"] = page_title.split(" - ")[1].split(" | LinkedIn")[0].strip()
                        print(f"Extracted title from page title: {profile_data['title']}")
                except:
                    pass

            if "title" not in profile_data or not profile_data["title"]:
                profile_data["title"] = ""
                print("⚠️ Could not find title element")

            # Company - try multiple selectors
            company_selectors = [
                ".pv-text-details__right-panel .inline-show-more-text",
                ".ph5 span.text-body-small.inline.t-black--light.break-words",
                ".pv-entity__secondary-title",
                ".pv-top-card--experience-list-item",
                ".pv-top-card-v2-section__entity-name",
                ".pv-top-card-v2-section__info-item",
                ".pv-recent-activity-section__card-subtitle",
                ".pv-top-card-section__company"
            ]

            print("Extracting company...")
            for selector in company_selectors:
                try:
                    company_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    profile_data["company"] = company_element.text.strip()
                    print(f"Found company: {profile_data['company']}")
                    break
                except:
                    continue

            if "company" not in profile_data or not profile_data["company"]:
                # Try an alternative approach - look at the experience section first item
                try:
                    # Scroll down a bit to ensure experience section is loaded
                    self.driver.execute_script("window.scrollBy(0, 500)")
                    time.sleep(2)

                    # Try to find the first company in experience
                    exp_company_selectors = [
                        ".experience-item .pv-entity__secondary-title",
                        ".pv-entity__company-summary-info h3",
                        ".pv-profile-section__card-item-v2 .pv-entity__secondary-title",
                        ".pvs-entity .pvs-entity__caption-wrapper"
                    ]

                    for selector in exp_company_selectors:
                        try:
                            exp_company = self.driver.find_element(By.CSS_SELECTOR, selector)
                            profile_data["company"] = exp_company.text.strip()
                            print(f"Found company from experience section: {profile_data['company']}")
                            break
                        except:
                            continue
                except:
                    pass

            if "company" not in profile_data or not profile_data["company"]:
                # Try location as fallback if company not found
                try:
                    location_selectors = [
                        ".pv-top-card-section__location",
                        ".pv-top-card--list-bullet li",
                        ".text-body-small.inline"
                    ]

                    for selector in location_selectors:
                        try:
                            location_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                            profile_data["company"] = location_element.text.strip()  # Use location as company
                            print(f"Using location as company: {profile_data['company']}")
                            break
                        except:
                            continue
                except:
                    profile_data["company"] = ""
                    print("⚠️ Could not find company element")

            # About section - scroll and try multiple approaches
            try:
                # Scroll down page to load more content
                print("Scrolling down to load more content...")
                for i in range(3):  # Scroll down in steps
                    self.driver.execute_script(f"window.scrollBy(0, {500 * (i + 1)})")
                    time.sleep(1)

                # Try to find about section - multiple approaches
                about_found = False

                # Try approach 1: Using ID
                try:
                    print("Looking for About section by ID...")
                    about_section = self.driver.find_element(By.CSS_SELECTOR, "#about")
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                                               about_section)
                    time.sleep(2)

                    # Look for the text content around the About section
                    about_element = None

                    # Try different selectors
                    about_selectors = [
                        "div#about + div + div",
                        "div#about + div div.display-flex",
                        "div#about ~ div .pv-shared-text-with-see-more"
                    ]

                    for selector in about_selectors:
                        try:
                            about_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                            break
                        except:
                            continue

                    if about_element:
                        # Try to click "see more" if it exists
                        try:
                            see_more_button = about_element.find_element(By.CSS_SELECTOR,
                                                                         ".inline-show-more-text__button")
                            print("Found 'See more' button in About section. Clicking...")
                            self.driver.execute_script("arguments[0].click();", see_more_button)
                            time.sleep(1)
                        except:
                            print("No 'See more' button found in About section")

                        profile_data["about"] = about_element.text.strip()
                        print(f"Found About section: {profile_data['about'][:50]}...")
                        about_found = True
                except Exception as e:
                    print(f"Approach 1 for About section failed: {str(e)}")

                # Try approach 2: Using section headers
                if not about_found:
                    try:
                        print("Looking for About section by section headers...")
                        section_headers = self.driver.find_elements(By.CSS_SELECTOR, ".section-title")

                        for header in section_headers:
                            if "About" in header.text:
                                # Found About section header
                                about_section = header.find_element(By.XPATH, "./ancestor::section")
                                about_content = about_section.find_element(By.CSS_SELECTOR,
                                                                           ".pv-shared-text-with-see-more")

                                # Try to click "see more" if it exists
                                try:
                                    see_more_button = about_content.find_element(By.CSS_SELECTOR,
                                                                                 ".inline-show-more-text__button")
                                    self.driver.execute_script("arguments[0].click();", see_more_button)
                                    time.sleep(1)
                                except:
                                    pass

                                profile_data["about"] = about_content.text.strip()
                                print(f"Found About section: {profile_data['about'][:50]}...")
                                about_found = True
                                break
                    except Exception as e:
                        print(f"Approach 2 for About section failed: {str(e)}")

                # Try approach 3: Look for text sections with headers
                if not about_found:
                    try:
                        print("Looking for About section by text content...")
                        headers = self.driver.find_elements(By.CSS_SELECTOR, ".pv-profile-section__card-heading")

                        for header in headers:
                            if "About" in header.text:
                                # Try to find the content associated with this header
                                parent_section = header.find_element(By.XPATH, "./ancestor::section")
                                text_containers = parent_section.find_elements(By.CSS_SELECTOR,
                                                                               ".pv-shared-text-with-see-more, .inline-show-more-text")

                                if text_containers:
                                    # Try to click "see more" if it exists
                                    try:
                                        see_more_button = text_containers[0].find_element(By.CSS_SELECTOR,
                                                                                          ".inline-show-more-text__button")
                                        self.driver.execute_script("arguments[0].click();", see_more_button)
                                        time.sleep(1)
                                    except:
                                        pass

                                    profile_data["about"] = text_containers[0].text.strip()
                                    print(f"Found About section: {profile_data['about'][:50]}...")
                                    about_found = True
                                    break
                    except Exception as e:
                        print(f"Approach 3 for About section failed: {str(e)}")

                if not about_found:
                    profile_data["about"] = ""
                    print("⚠️ Could not find About section")

            except Exception as e:
                profile_data["about"] = ""
                print(f"⚠️ Error extracting About section: {str(e)}")

            # Experience - try multiple approaches
            try:
                print("Scrolling to find Experience section...")
                # Scroll down more to ensure experience section is loaded
                self.driver.execute_script("window.scrollBy(0, 800)")
                time.sleep(2)

                experience_found = False

                # Try approach 1: Using ID
                try:
                    experience_section = self.driver.find_element(By.ID, "experience")
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                                               experience_section)
                    time.sleep(2)

                    # Try different selectors for experience items
                    experience_selectors = [
                        ".pvs-list__item--line-separated",
                        ".pv-entity__position-group",
                        ".pv-profile-section__list-item",
                        ".pvs-entity",
                        ".pv-profile-section experience-section ul > li"
                    ]

                    for selector in experience_selectors:
                        try:
                            experience_items = experience_section.find_elements(By.CSS_SELECTOR, selector)
                            if experience_items:
                                experiences = []
                                for i, item in enumerate(experience_items[:3]):  # Limit to 3 most recent experiences
                                    experiences.append(item.text.strip())
                                    print(f"Found experience {i + 1}: {item.text[:50]}...")

                                profile_data["experience"] = "\n".join(experiences)
                                experience_found = True
                                break
                        except:
                            continue
                except Exception as e:
                    print(f"Approach 1 for Experience section failed: {str(e)}")

                # Try approach 2: Using section headers
                if not experience_found:
                    try:
                        section_headers = self.driver.find_elements(By.CSS_SELECTOR, ".section-title")

                        for header in section_headers:
                            if "Experience" in header.text:
                                # Found Experience section header
                                exp_section = header.find_element(By.XPATH, "./ancestor::section")
                                exp_items = exp_section.find_elements(By.CSS_SELECTOR, ".pv-profile-section__list-item")

                                experiences = []
                                for i, item in enumerate(exp_items[:3]):  # Limit to 3 most recent experiences
                                    experiences.append(item.text.strip())
                                    print(f"Found experience {i + 1}: {item.text[:50]}...")

                                profile_data["experience"] = "\n".join(experiences)
                                experience_found = True
                                break
                    except Exception as e:
                        print(f"Approach 2 for Experience section failed: {str(e)}")

                # Try approach 3: Look for any experience-related content
                if not experience_found:
                    try:
                        print("Searching for experience content by keywords...")
                        # Look for sections that might contain job titles and companies
                        sections = self.driver.find_elements(By.CSS_SELECTOR, ".artdeco-card")

                        for section in sections:
                            try:
                                section_text = section.text.lower()
                                if "experience" in section_text or "work" in section_text or "job" in section_text:
                                    # This section might be related to experience
                                    list_items = section.find_elements(By.CSS_SELECTOR, "li")
                                    if list_items:
                                        experiences = []
                                        for i, item in enumerate(list_items[:3]):
                                            if len(item.text.strip()) > 10:  # Avoid empty or very small items
                                                experiences.append(item.text.strip())
                                                print(f"Found potential experience {i + 1}: {item.text[:50]}...")

                                        if experiences:
                                            profile_data["experience"] = "\n".join(experiences)
                                            experience_found = True
                                            break
                            except:
                                continue
                    except Exception as e:
                        print(f"Approach 3 for Experience section failed: {str(e)}")

                if not experience_found:
                    profile_data["experience"] = ""
                    print("⚠️ Could not find Experience section")

            except Exception as e:
                profile_data["experience"] = ""
                print(f"⚠️ Error extracting Experience section: {str(e)}")

            # If we have at least name or title, consider it a successful scrape
            if profile_data.get("name") or profile_data.get("title"):
                print("✅ Profile data extraction complete")
                return profile_data
            else:
                # Take a screenshot for debugging
                timestamp = int(time.time())
                screenshot_path = f"profile_debug_{timestamp}.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"Insufficient data extracted. Screenshot saved to {screenshot_path}")

                # Return whatever we managed to extract
                if any(profile_data.values()):
                    print("Returning partial profile data")
                    return profile_data
                else:
                    print("No data could be extracted")
                    return {}

        except TimeoutException:
            print(f"⚠️ Timeout while scraping profile: {profile_url}")

            # Take a screenshot for debugging
            timestamp = int(time.time())
            screenshot_path = f"timeout_debug_{timestamp}.png"
            self.driver.save_screenshot(screenshot_path)
            print(f"Timeout screenshot saved to {screenshot_path}")

            return {}
        except Exception as e:
            print(f"❌ Error scraping profile {profile_url}: {str(e)}")

            # Take a screenshot for debugging
            timestamp = int(time.time())
            screenshot_path = f"error_debug_{timestamp}.png"
            self.driver.save_screenshot(screenshot_path)
            print(f"Error screenshot saved to {screenshot_path}")

            return {}

    def close(self):
        """Close the WebDriver"""
        print("Closing Chrome browser...")
        self.driver.quit()
        print("Browser closed successfully")


def generate_summary(profile_data):
    """Generate a summary of the LinkedIn profile using OpenAI"""
    try:
        print("Generating summary using OpenAI...")

        # Check if we have at least some data to work with
        if not profile_data or (not profile_data.get("name") and not profile_data.get("title")):
            print("⚠️ Insufficient profile data for summary generation")
            return ""

        # Create prompt with available data
        prompt_parts = [
            "Create a professional summary for a sales outreach based on this LinkedIn profile information:"
        ]

        # Add all profile data we have
        for key, value in profile_data.items():
            if value:
                prompt_parts.append(f"{key.capitalize()}: {value}")

        # Add instructions
        prompt_parts.append(
            "The summary should be concise (2-3 sentences) and highlight the person's current role, experience, "
            "and any relevant background that would be useful for sales outreach. Focus on their professional "
            "capabilities and decision-making authority. If some information is missing, focus on what is available."
        )

        prompt = "\n\n".join(prompt_parts)

        print("Sending request to OpenAI API...")

        # Use the new OpenAI API format (v1.0.0+)
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "You are an assistant that creates concise professional summaries."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=250,
            temperature=0.7
        )

        summary = response.choices[0].message.content.strip()
        print(f"Summary generated: {summary[:100]}...")
        return summary

    except Exception as e:
        print(f"❌ Error generating summary: {str(e)}")
        return f"[ERROR: {str(e)}]"  # Return error message instead of empty string


def process_csv(input_file, output_file, linkedin_scraper, batch_size=5, start_row=0):
    """Process the CSV file in batches to handle large datasets"""
    # Read the CSV file
    print(f"Reading input CSV: {input_file}")

    # Verify file exists
    if not os.path.exists(input_file):
        print(f"❌ Error: Input file '{input_file}' not found!")
        print(f"Current directory: {os.getcwd()}")
        print("Please check the file path and try again.")
        return None

    try:
        df = pd.read_csv(input_file)
        print(f"CSV loaded successfully with {len(df)} rows and {len(df.columns)} columns")

        # Verify LinkedIn Profile column exists
        if "LinkedIn Profile" not in df.columns:
            print(f"❌ Error: CSV file does not contain 'LinkedIn Profile' column!")
            print(f"Available columns: {', '.join(df.columns)}")
            return None

        # Make sure LinkedIn Summary column exists and is properly initialized
        if "LinkedIn Summary" not in df.columns:
            df["LinkedIn Summary"] = ""
            print("Added 'LinkedIn Summary' column to the dataframe")
        else:
            # Convert any NaN values to empty strings to avoid issues
            df["LinkedIn Summary"] = df["LinkedIn Summary"].fillna("")
            print("Initialized existing LinkedIn Summary column")

        # Print first few rows for verification
        print("\nSample of LinkedIn URLs from CSV:")
        for i in range(min(3, len(df))):
            url = df.iloc[i]["LinkedIn Profile"] if not pd.isna(df.iloc[i]["LinkedIn Profile"]) else "N/A"
            print(f"  Row {i + 1}: {url}")

        # Track progress
        total_rows = len(df)
        processed_rows = 0

        print(f"\nTotal rows in CSV: {total_rows}")
        print(f"Starting from row: {start_row}")
        print(f"Batch size: {batch_size}")

        # Add safety code to prevent excessive rate limiting
        consecutive_failures = 0
        max_consecutive_failures = 3

        # Process in batches
        for i in range(start_row, total_rows, batch_size):
            batch_end = min(i + batch_size, total_rows)
            print(f"\n{'=' * 50}")
            print(f"Processing batch {i // batch_size + 1}: rows {i} to {batch_end - 1}")
            print(f"{'=' * 50}")

            # Process each row in the batch
            for j in range(i, batch_end):
                try:
                    print(f"\n{'*' * 30}")
                    print(f"Row {j + 1}/{total_rows}: Processing")

                    # Get LinkedIn URL
                    linkedin_url = df.iloc[j]["LinkedIn Profile"]

                    # Skip if URL is missing or already has a summary
                    if pd.isna(linkedin_url) or linkedin_url == "":
                        print(f"⚠️ Row {j + 1}: No LinkedIn URL, skipping")
                        continue

                    if not pd.isna(df.iloc[j]["LinkedIn Summary"]) and df.iloc[j]["LinkedIn Summary"] != "":
                        print(f"⚠️ Row {j + 1}: Already has a summary, skipping")
                        continue

                    print(f"Processing URL: {linkedin_url}")

                    # Scrape profile with retry mechanism
                    profile_data = {}
                    max_retries = 2

                    for retry in range(max_retries + 1):
                        if retry > 0:
                            print(f"Retry attempt {retry}/{max_retries}...")
                            # Take a longer break before retrying
                            retry_delay = random.uniform(15, 30)
                            print(f"Waiting {retry_delay:.2f} seconds before retry...")
                            time.sleep(retry_delay)

                        profile_data = linkedin_scraper.scrape_profile(linkedin_url)

                        if profile_data and (profile_data.get("name") or profile_data.get("title")):
                            # Success - reset consecutive failures counter
                            consecutive_failures = 0
                            break
                        elif retry < max_retries:
                            print(f"Retry {retry + 1}/{max_retries} for profile: {linkedin_url}")
                        else:
                            # Failed all retries
                            consecutive_failures += 1

                    # Generate summary if profile data was obtained
                    if profile_data and (profile_data.get("name") or profile_data.get("title")):
                        try:
                            summary = generate_summary(profile_data)
                            if summary:
                                # Directly update the DataFrame with the summary
                                df.at[j, "LinkedIn Summary"] = summary

                                # Immediately save the DataFrame to CSV after each summary
                                # to ensure changes are persisted
                                df.to_csv(output_file, index=False)

                                print(f"✅ Row {j + 1}: Summary generated and saved")

                                # Verify the summary was saved by reading it back
                                try:
                                    verify_df = pd.read_csv(output_file)
                                    saved_summary = verify_df.iloc[j]["LinkedIn Summary"] if j < len(
                                        verify_df) else None
                                    if saved_summary and saved_summary == summary:
                                        print(f"✓ Verified summary was saved correctly")
                                    else:
                                        print(f"⚠️ Summary may not have been saved correctly")
                                except Exception as e:
                                    print(f"⚠️ Could not verify saved summary: {str(e)}")
                            else:
                                print(f"⚠️ Row {j + 1}: Failed to generate summary")
                                df.at[j, "LinkedIn Summary"] = "[SUMMARY GENERATION FAILED]"
                                df.to_csv(output_file, index=False)
                        except Exception as e:
                            print(f"❌ Row {j + 1}: Error generating summary: {str(e)}")
                            df.at[j, "LinkedIn Summary"] = f"[SUMMARY ERROR: {str(e)}]"
                            df.to_csv(output_file, index=False)
                    else:
                        print(f"❌ Row {j + 1}: Failed to scrape profile after retries")
                        # Add a placeholder to indicate this was processed but failed
                        df.at[j, "LinkedIn Summary"] = "[SCRAPING FAILED]"
                        df.to_csv(output_file, index=False)

                    # Safety check - take longer break if too many consecutive failures
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"\n⚠️ WARNING: {consecutive_failures} consecutive failures detected!")
                        print("LinkedIn may be rate limiting or blocking the scraper.")
                        print("Taking an extended break to avoid IP ban...")

                        # Save progress before extended break
                        df.to_csv(output_file, index=False)
                        print(f"Progress saved to {output_file}")

                        # Long cooling off period
                        cooling_period = random.uniform(300, 600)  # 5-10 minutes
                        print(f"Cooling off for {cooling_period / 60:.1f} minutes...")
                        time.sleep(cooling_period)

                        # Reset counter after break
                        consecutive_failures = 0

                    # Add random delay between profiles - longer than before
                    if j < batch_end - 1:  # Skip delay after last profile in batch
                        delay = random.uniform(10, 20)  # Increased from 2-5 to 10-20 seconds
                        print(f"Adding delay of {delay:.2f} seconds before next profile...")
                        time.sleep(delay)

                except Exception as e:
                    print(f"❌ Error processing row {j + 1}: {str(e)}")
                    # Save on error to preserve progress
                    df.to_csv(output_file, index=False)

            # Save progress after each batch
            print(f"\nSaving progress to {output_file}...")
            df.to_csv(output_file, index=False)
            processed_rows += batch_end - i
            completion_percentage = processed_rows / total_rows * 100
            print(f"Progress: {processed_rows}/{total_rows} rows processed ({completion_percentage:.2f}%)")

            # Longer delay between batches to avoid rate limiting
            if batch_end < total_rows:
                delay = random.uniform(45, 90)  # Increased from 15-30 to 45-90 seconds
                print(f"Taking a longer break for {delay:.2f} seconds between batches...")
                time.sleep(delay)

        return df

    except Exception as e:
        print(f"❌ Error processing CSV: {str(e)}")
        return None


def main():
    """Main function to run the LinkedIn scraper"""
    print("\n" + "=" * 70)
    print("Starting LinkedIn Profile Scraper and Summary Generator")
    print("=" * 70 + "\n")

    # Debug information
    print(f"Python version: {platform.python_version()}")
    print(f"Current working directory: {os.getcwd()}")

    # Check for OpenAI API key
    if os.getenv("OPENAI_API_KEY"):
        print("✅ OpenAI API key found")
    else:
        print("❌ OpenAI API key not found - check your .env file")

    # Check for LinkedIn credentials
    if os.getenv("LINKEDIN_EMAIL") and os.getenv("LINKEDIN_PASSWORD"):
        print("✅ LinkedIn credentials found")
    else:
        print("❌ LinkedIn credentials not found - check your .env file")

    # Load configuration from environment variables
    linkedin_email = os.getenv("LINKEDIN_EMAIL")
    linkedin_password = os.getenv("LINKEDIN_PASSWORD")
    input_file = os.getenv("INPUT_CSV", "input.csv")
    output_file = os.getenv("OUTPUT_CSV", "leads_with_summaries.csv")
    batch_size = int(os.getenv("BATCH_SIZE", "5"))  # Reduced default batch size from 10 to 5
    start_row = int(os.getenv("START_ROW", "0"))

    # Check for input file
    if os.path.exists(input_file):
        print(f"✅ Input file found: {input_file}")
    else:
        print(f"❌ Input file not found: {input_file}")

    # Check if required environment variables are set
    if not all([linkedin_email, linkedin_password, input_file]):
        print("❌ Error: Required environment variables not set.")
        print("Please set LINKEDIN_EMAIL, LINKEDIN_PASSWORD, and INPUT_CSV in your .env file.")
        return

    print(f"Configuration:")
    print(f"- Input CSV: {input_file}")
    print(f"- Output CSV: {output_file}")
    print(f"- Batch Size: {batch_size}")
    print(f"- Starting Row: {start_row}")

    # Prompt user to confirm
    try:
        confirmation = input("\nReady to begin scraping LinkedIn profiles? (y/n): ")
        if confirmation.lower() != 'y':
            print("Operation cancelled by user.")
            return
    except:
        # If running in an environment without input, proceed anyway
        pass

    try:
        # Initialize the LinkedIn scraper
        print("\nInitializing LinkedIn scraper...")
        scraper = LinkedInScraper(linkedin_email, linkedin_password)

        # Process the CSV file
        print("\nBeginning CSV processing...")
        df = process_csv(input_file, output_file, scraper, batch_size, start_row)

        # Verify summaries were saved
        if df is not None:
            # Verify summaries were saved by checking the final output file
            print("\nVerifying saved summaries...")
            try:
                verification_df = pd.read_csv(output_file)

                summaries_found = verification_df["LinkedIn Summary"].notna() & (
                            verification_df["LinkedIn Summary"] != "")
                summary_count = summaries_found.sum()

                print(f"Found {summary_count} saved summaries in the output file")

                if summary_count > 0:
                    print("\nSample of saved summaries:")
                    sample_df = verification_df[summaries_found].head(3)
                    for i, row in sample_df.iterrows():
                        name = f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
                        if not name:
                            name = row.get('Full Name', 'Unknown')
                        summary = row['LinkedIn Summary']
                        print(f"\nProfile: {name}")
                        print(f"Summary: {summary}")

                print(f"\n✅ Processing complete. Results saved to {output_file}")
            except Exception as e:
                print(f"⚠️ Error verifying summaries: {str(e)}")
                print(f"Output file should still be saved at: {output_file}")
        else:
            print("\n❌ Processing failed. Check the error messages above.")

    except Exception as e:
        print(f"\n❌ An error occurred: {str(e)}")

    finally:
        # Close the scraper to release resources
        if 'scraper' in locals():
            scraper.close()

    print("\n" + "=" * 70)
    print("LinkedIn Profile Scraper and Summary Generator Finished")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()