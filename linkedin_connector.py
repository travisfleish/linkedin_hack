import os
import time
import random
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from openai import OpenAI
import platform

# Load environment variables
load_dotenv()


def generate_personalized_note(profile_data, character_limit=300):
    """
    Generate a personalized connection note based on LinkedIn profile data using OpenAI

    Args:
        profile_data (dict): Data extracted from LinkedIn profile
        character_limit (int): Maximum characters for the note

    Returns:
        str: Personalized connection note
    """
    try:
        # Initialize OpenAI client
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        if not client:
            print("⚠️ OpenAI client initialization failed")
            return None

        # Create a prompt for the personalized note with improved guidelines
        prompt = f"""
        Create a brief, friendly LinkedIn connection request note (max {character_limit} characters) based on this profile:

        Name: {profile_data.get('name', 'a professional')}
        Title: {profile_data.get('title', '')}
        Experience: {profile_data.get('experience', '')}

        The note should follow these EXACT guidelines:
        1. Start with "Hi [FirstName]," 
        2. Briefly mention something about their professional background or role (NOT their specific company)
        3. Mention that you work with AI applications in sports
        4. Give a brief reason for connecting, but do NOT suggest having a call or further discussion
        5. Keep it casual and genuine, NOT overly complimentary or flattering
        6. Be concise (under {character_limit} characters)
        7. Do NOT include any explicit call to action at the end
        8. The tone should be professional but conversational

        Here's an example of the ideal format and tone:
        "Hi [Name], your experience with performance optimization in professional sports is intriguing. I'm currently working with AI applications in sports and thought connecting would be valuable as we're both in the training and technology space."

        Format as plain text without quotation marks.
        """

        # Generate the note using OpenAI
        response = client.chat.completions.create(
            model="gpt-4-turbo",  # Or a more appropriate model
            messages=[
                {"role": "system",
                 "content": "You create natural, brief networking notes that sound like they're written by a real person, not an AI."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150,
            temperature=0.7
        )

        # Extract and clean the generated note
        note = response.choices[0].message.content.strip()

        # Ensure note is within character limit
        if len(note) > character_limit:
            note = note[:character_limit - 3] + "..."

        print(f"Generated personalized note ({len(note)} chars): {note}")
        return note

    except Exception as e:
        print(f"❌ Error generating personalized note: {str(e)}")
        return None


class LinkedInConnector:
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

    def extract_profile_data(self, wait_time=3):
        """
        Extract key data from the current LinkedIn profile page

        Args:
            wait_time (int): Additional time to wait for page elements to load

        Returns:
            dict: Extracted profile data
        """
        profile_data = {
            "name": "",
            "title": "",
            "company": "",
            "experience": ""
        }

        try:
            # Wait for profile to load
            time.sleep(wait_time)

            # Extract name - try multiple selectors
            name_selectors = [
                "h1.text-heading-xlarge",
                "h1.inline.t-24.t-black.t-normal.break-words",
                ".pv-top-card--list li:first-child",
                "h1.text-heading-large",
                ".text-heading-xlarge",
                "h1"  # Most generic - try this last
            ]

            for selector in name_selectors:
                try:
                    name_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    profile_data["name"] = name_element.text.strip()
                    break
                except:
                    continue

            # If all selectors fail, try to extract name from page title
            if not profile_data["name"]:
                try:
                    page_title = self.driver.title
                    if " - " in page_title and " | LinkedIn" in page_title:
                        profile_data["name"] = page_title.split(" - ")[0].strip()
                    elif " | LinkedIn" in page_title:
                        profile_data["name"] = page_title.split(" | LinkedIn")[0].strip()
                except:
                    pass

            # Extract title - try multiple selectors
            title_selectors = [
                ".text-body-medium.break-words",
                ".pv-top-card--list li:nth-child(2)",
                ".ph5 div.display-flex.flex-wrap.align-items-center div span",
                "[data-field='headline']",
                ".pv-top-card .text-body-medium"
            ]

            for selector in title_selectors:
                try:
                    title_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    profile_data["title"] = title_element.text.strip()
                    break
                except:
                    continue

            # Extract company - try multiple approaches
            company_selectors = [
                ".pv-text-details__right-panel .inline-show-more-text",
                ".ph5 span.text-body-small.inline.t-black--light.break-words",
                ".pv-entity__secondary-title",
                ".pv-top-card--experience-list-item"
            ]

            for selector in company_selectors:
                try:
                    company_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    profile_data["company"] = company_element.text.strip()
                    break
                except:
                    continue

            # Try to extract recent experience
            try:
                # Scroll down to load more content
                self.driver.execute_script("window.scrollBy(0, 500)")
                time.sleep(1)

                # Look for experience section
                experience_selectors = [
                    ".experience-section",
                    "#experience-section",
                    "section.pv-profile-section.experience-section",
                    "#experience"
                ]

                for selector in experience_selectors:
                    try:
                        exp_section = self.driver.find_element(By.CSS_SELECTOR, selector)
                        exp_items = exp_section.find_elements(By.CSS_SELECTOR, ".pv-entity__summary-info")

                        if exp_items:
                            profile_data["experience"] = exp_items[0].text.strip()
                        break
                    except:
                        continue
            except:
                pass

            return profile_data

        except Exception as e:
            print(f"Error extracting profile data: {str(e)}")
            return profile_data

    def send_connection_request(self, profile_url, personalized_note=None, use_ai_note=False):
        """
        Send a connection request to a LinkedIn profile

        Args:
            profile_url (str): URL of the LinkedIn profile
            personalized_note (str, optional): Custom note to add to connection request
            use_ai_note (bool): Whether to generate a note using OpenAI based on profile data

        Returns:
            dict: Result including status and success flag
        """
        try:
            # Add random delay before visiting profile
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
                time.sleep(random.uniform(3, 5))

            # Wait for profile to load with more reliable indicators
            print("Waiting for profile to load completely...")
            for selector in [".pv-top-card", "h1", ".artdeco-card"]:
                try:
                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    print(f"Profile loaded successfully (detected {selector})")
                    break
                except:
                    continue

            time.sleep(random.uniform(1, 2))  # Small additional delay

            # Check if already connected or connection pending
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            if "Message" in page_text and "Connect" not in page_text:
                print("Already connected to this profile")
                return {"status": "already_connected", "success": True}
            if "Pending" in page_text:
                print("Connection request already pending")
                return {"status": "already_pending", "success": True}

            # Extract profile data for AI-generated note if needed
            if use_ai_note and not personalized_note:
                print("Extracting profile data for AI-generated note...")
                profile_data = self.extract_profile_data()

                # Generate personalized note using OpenAI
                if any(profile_data.values()):
                    print("Generating personalized note using AI...")
                    ai_note = generate_personalized_note(profile_data)
                    if ai_note:
                        personalized_note = ai_note
                        print(f"Using AI-generated note: {personalized_note}")

            # First look for direct Connect button
            print("Looking for direct Connect button...")
            direct_connect_button = None

            # Try CSS selectors first
            connect_selectors = [
                "button.pv-s-profile-actions--connect",
                "button[aria-label='Connect']",
                "button.artdeco-button--primary[aria-label*='Connect']",
                ".pvs-profile-actions button[aria-label*='Connect']"
            ]

            for selector in connect_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_displayed() and ("Connect" in element.text or
                                                       (element.get_attribute("aria-label") and
                                                        "connect" in element.get_attribute("aria-label").lower())):
                            direct_connect_button = element
                            break
                    if direct_connect_button:
                        break
                except:
                    continue

            # Try XPath for direct connect button
            if not direct_connect_button:
                try:
                    connect_xpath = "//button[contains(., 'Connect') or @aria-label='Connect']"
                    connect_elements = self.driver.find_elements(By.XPATH, connect_xpath)
                    for element in connect_elements:
                        if element.is_displayed():
                            direct_connect_button = element
                            break
                except:
                    pass

            # If direct Connect button not found, try looking for More button
            if not direct_connect_button:
                print("Direct Connect button not found. Looking for More dropdown...")
                more_button = None

                # Try CSS selectors for More button
                more_selectors = [
                    "button.artdeco-dropdown__trigger[aria-label='More']",
                    ".pvs-profile-actions button:nth-child(3)",  # Often the third button is More
                    "button.artdeco-dropdown__trigger",
                    "button[aria-label='More actions']",
                    ".pv-s-profile-actions__overflow-toggle"
                ]

                for selector in more_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            if element.is_displayed() and ("More" in element.text or
                                                           (element.get_attribute("aria-label") and
                                                            ("more" in element.get_attribute("aria-label").lower() or
                                                             "actions" in element.get_attribute(
                                                                        "aria-label").lower()))):
                                more_button = element
                                break
                        if more_button:
                            break
                    except:
                        continue

                # Try XPath for more button
                if not more_button:
                    try:
                        more_xpath = "//button[contains(., 'More') or @aria-label='More' or @aria-label='More actions']"
                        more_elements = self.driver.find_elements(By.XPATH, more_xpath)
                        for element in more_elements:
                            if element.is_displayed():
                                more_button = element
                                break
                    except:
                        pass

                # If still not found, try another approach
                if not more_button:
                    try:
                        # Often the more button is the 3rd button in the profile actions section
                        action_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".pvs-profile-actions button")
                        if len(action_buttons) >= 3:
                            more_button = action_buttons[2]  # The 3rd button (index 2)
                    except:
                        pass

                # If More button found, click it and then look for Connect option
                if more_button:
                    print("More dropdown button found. Clicking...")
                    self.driver.execute_script("arguments[0].click();", more_button)
                    time.sleep(1.5)  # Wait for dropdown to appear

                    # Look for Connect option in dropdown
                    connect_option = None

                    # Try CSS selectors for connect option in dropdown
                    dropdown_selectors = [
                        ".artdeco-dropdown__content-inner li a[aria-label*='Connect']",
                        ".artdeco-dropdown__content li[aria-label*='Connect']",
                        ".artdeco-dropdown__content button[aria-label*='Connect']",
                        ".artdeco-dropdown__content-inner div[role='button']",
                        ".artdeco-dropdown__content-inner li:nth-child(1)"  # Often the first option is Connect
                    ]

                    for selector in dropdown_selectors:
                        try:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for element in elements:
                                if element.is_displayed() and ("Connect" in element.text or
                                                               (element.get_attribute("aria-label") and
                                                                "connect" in element.get_attribute(
                                                                           "aria-label").lower())):
                                    connect_option = element
                                    break
                            if connect_option:
                                break
                        except:
                            continue

                    # Try XPath for connect option
                    if not connect_option:
                        try:
                            connect_xpath = "//li[contains(., 'Connect')]"
                            connect_elements = self.driver.find_elements(By.XPATH, connect_xpath)
                            for element in connect_elements:
                                if element.is_displayed():
                                    connect_option = element
                                    break
                        except:
                            pass

                    if connect_option:
                        print("Connect option found in dropdown. Clicking...")
                        self.driver.execute_script("arguments[0].click();", connect_option)
                        time.sleep(1.5)  # Wait for connect dialog
                    else:
                        print("⚠️ Connect option not found in More dropdown")
                        # Take a screenshot for debugging
                        timestamp = int(time.time())
                        screenshot_path = f"dropdown_debug_{timestamp}.png"
                        self.driver.save_screenshot(screenshot_path)
                        print(f"Screenshot saved to {screenshot_path}")
                        return {"status": "connect_option_not_found", "success": False}
                else:
                    print("⚠️ More button not found")
                    # Take a screenshot for debugging
                    timestamp = int(time.time())
                    screenshot_path = f"more_button_debug_{timestamp}.png"
                    self.driver.save_screenshot(screenshot_path)
                    print(f"Screenshot saved to {screenshot_path}")
                    return {"status": "more_button_not_found", "success": False}
            else:
                # If direct Connect button was found, click it
                print("Direct Connect button found. Clicking...")
                self.driver.execute_script("arguments[0].click();", direct_connect_button)
                time.sleep(1.5)  # Wait for connect dialog

            # Now we should have the connection dialog open, handle the note if needed
            if personalized_note:
                try:
                    # Look for "Add a note" button or similar
                    add_note_button = None

                    # Try CSS selectors for Add note button
                    add_note_selectors = [
                        "button.artdeco-button[aria-label*='Add a note']",
                        "button:contains('Add a note')",
                        ".artdeco-modal-footer button.artdeco-button--secondary",
                        ".artdeco-modal__actionbar button:nth-child(1)"  # Often the first button in the footer
                    ]

                    for selector in add_note_selectors:
                        try:
                            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for element in elements:
                                if element.is_displayed() and ("Add a note" in element.text or
                                                               (element.get_attribute("aria-label") and
                                                                "add a note" in element.get_attribute(
                                                                           "aria-label").lower())):
                                    add_note_button = element
                                    break
                            if add_note_button:
                                break
                        except:
                            continue

                    # Try XPath for add note button
                    if not add_note_button:
                        try:
                            add_note_xpath = "//button[contains(., 'Add a note')]"
                            add_note_elements = self.driver.find_elements(By.XPATH, add_note_xpath)
                            for element in add_note_elements:
                                if element.is_displayed():
                                    add_note_button = element
                                    break
                        except:
                            pass

                    if add_note_button:
                        print("Clicking 'Add a note' button...")
                        self.driver.execute_script("arguments[0].click();", add_note_button)
                        time.sleep(1)

                        # Find and enter text in the note textarea
                        note_textarea = None

                        # Try CSS selectors for textarea
                        textarea_selectors = [
                            "textarea#custom-message",
                            ".artdeco-modal textarea",
                            "textarea[name='message']",
                            ".send-invite__custom-message",
                            "textarea"
                        ]

                        for selector in textarea_selectors:
                            try:
                                textarea_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                for element in textarea_elements:
                                    if element.is_displayed():
                                        note_textarea = element
                                        break
                                if note_textarea:
                                    break
                            except:
                                continue

                        if note_textarea:
                            note_textarea.clear()
                            # Type slowly like a human
                            for char in personalized_note:
                                note_textarea.send_keys(char)
                                time.sleep(random.uniform(0.01, 0.05))  # Slight delay between keystrokes
                            time.sleep(1)
                        else:
                            print("⚠️ Could not find note textarea")
                    else:
                        print("⚠️ Could not find 'Add a note' button")
                except Exception as e:
                    print(f"⚠️ Could not add personalized note: {str(e)}")

            # Finally, click Send/Done button
            try:
                send_button = None

                # Try CSS selectors for send button
                send_selectors = [
                    "button.artdeco-button--primary[aria-label*='Send']",
                    "button:contains('Send')",
                    ".artdeco-modal-footer button.artdeco-button--primary",
                    "button[aria-label='Send now']",
                    ".artdeco-modal__actionbar button:nth-child(2)"  # Often the second button in the footer
                ]

                for selector in send_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            if element.is_displayed() and ("Send" in element.text or
                                                           (element.get_attribute("aria-label") and
                                                            "send" in element.get_attribute("aria-label").lower())):
                                send_button = element
                                break
                        if send_button:
                            break
                    except:
                        continue

                # Try XPath for send button
                if not send_button:
                    try:
                        send_xpath = "//button[contains(., 'Send')]"
                        send_elements = self.driver.find_elements(By.XPATH, send_xpath)
                        for element in send_elements:
                            if element.is_displayed():
                                send_button = element
                                break
                    except:
                        pass

                if send_button:
                    print("Clicking 'Send' button...")
                    self.driver.execute_script("arguments[0].click();", send_button)
                    time.sleep(2)
                    print("✅ Connection request sent successfully")
                    return {"status": "request_sent", "success": True}
                else:
                    print("⚠️ Send button not found")
                    # Take a screenshot for debugging
                    timestamp = int(time.time())
                    screenshot_path = f"send_button_debug_{timestamp}.png"
                    self.driver.save_screenshot(screenshot_path)
                    print(f"Screenshot saved to {screenshot_path}")
                    return {"status": "send_button_not_found", "success": False}
            except Exception as e:
                print(f"❌ Error sending connection request: {str(e)}")
                return {"status": f"error: {str(e)}", "success": False}

        except Exception as e:
            print(f"❌ Error sending connection request: {str(e)}")

            # Take a screenshot for debugging
            timestamp = int(time.time())
            screenshot_path = f"error_debug_{timestamp}.png"
            self.driver.save_screenshot(screenshot_path)
            print(f"Error screenshot saved to {screenshot_path}")

            return {"status": f"error: {str(e)}", "success": False}

    def close(self):
        """Close the WebDriver"""
        print("Closing Chrome browser...")
        self.driver.quit()
        print("Browser closed successfully")


def process_connections(input_file, output_file, connector, batch_size=5,
                        start_row=0, max_requests=20,  # Reduced default to 20
                        personalized_note_template=None, use_ai_notes=False):
    """
    Process the CSV file and send connection requests with safety measures

    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
        connector (LinkedInConnector): Instance of LinkedIn connector
        batch_size (int): Number of profiles to process before taking a longer break
        start_row (int): Row to start processing from
        max_requests (int): Maximum number of requests to send
        personalized_note_template (str, optional): Template for personalized notes
        use_ai_notes (bool): Whether to use AI to generate personalized notes

    Returns:
        DataFrame: Updated DataFrame with connection request status
    """
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

        # Add or ensure connection tracking columns exist
        if "Connection Status" not in df.columns:
            df["Connection Status"] = ""
            print("Added 'Connection Status' column to the dataframe")

        if "Connection Date" not in df.columns:
            df["Connection Date"] = ""
            print("Added 'Connection Date' column to the dataframe")

        if "Connection Note" not in df.columns:
            df["Connection Note"] = ""
            print("Added 'Connection Note' column to the dataframe")

        # Track progress
        total_rows = len(df)
        processed_rows = 0
        requests_sent = 0
        requests_failed = 0
        consecutive_errors = 0
        max_consecutive_errors = 3

        print(f"\nTotal rows in CSV: {total_rows}")
        print(f"Starting from row: {start_row}")
        print(f"Batch size: {batch_size}")
        print(f"Maximum requests: {max_requests}")
        print(f"Using AI-generated notes: {use_ai_notes}")

        # Get current date for tracking
        current_date = time.strftime("%Y-%m-%d")

        # Process in batches
        for i in range(start_row, total_rows, batch_size):
            if requests_sent >= max_requests:
                print(f"\n⚠️ Maximum requests reached ({max_requests})")
                print("Saving progress and exiting...")
                df.to_csv(output_file, index=False)
                return df

            batch_end = min(i + batch_size, total_rows)
            print(f"\n{'=' * 50}")
            print(f"Processing batch {i // batch_size + 1}: rows {i} to {batch_end - 1}")
            print(f"{'=' * 50}")

            # Process each row in the batch
            for j in range(i, batch_end):
                # Check if we've reached the limit
                if requests_sent >= max_requests:
                    print(f"\n⚠️ Maximum requests reached ({max_requests})")
                    break

                # Safety check - take longer break if too many consecutive errors
                if consecutive_errors >= max_consecutive_errors:
                    print(f"\n⚠️ Too many consecutive errors ({consecutive_errors}).")
                    print("Taking a longer break before continuing...")
                    time.sleep(random.uniform(300, 600))  # 5-10 minute break
                    consecutive_errors = 0  # Reset counter

                try:
                    print(f"\n{'*' * 30}")
                    print(f"Row {j + 1}/{total_rows}: Processing")

                    # Get LinkedIn URL
                    linkedin_url = df.iloc[j]["LinkedIn Profile"]

                    # Skip if URL is missing
                    if pd.isna(linkedin_url) or linkedin_url == "":
                        print(f"⚠️ Row {j + 1}: No LinkedIn URL, skipping")
                        continue

                    # Skip if already processed
                    if not pd.isna(df.iloc[j]["Connection Status"]) and df.iloc[j]["Connection Status"] != "":
                        print(f"⚠️ Row {j + 1}: Already processed, skipping")
                        continue

                    print(f"Processing URL: {linkedin_url}")

                    # Create personalized note if template provided and not using AI
                    personalized_note = None
                    if personalized_note_template and not use_ai_notes:
                        # Get data for personalization
                        first_name = df.iloc[j].get("First Name", "")
                        last_name = df.iloc[j].get("Last Name", "")
                        full_name = df.iloc[j].get("Full Name", "")
                        company = df.iloc[j].get("Company Name", "")
                        title = df.iloc[j].get("Job Title", "")

                        if not first_name and full_name:
                            # Try to extract first name from full name
                            name_parts = full_name.split()
                            if name_parts:
                                first_name = name_parts[0]

                        # Replace placeholders in template
                        personalized_note = personalized_note_template
                        if "{first_name}" in personalized_note and first_name:
                            personalized_note = personalized_note.replace("{first_name}", first_name)
                        if "{last_name}" in personalized_note and last_name:
                            personalized_note = personalized_note.replace("{last_name}", last_name)
                        if "{full_name}" in personalized_note and full_name:
                            personalized_note = personalized_note.replace("{full_name}", full_name)
                        if "{company}" in personalized_note and company:
                            personalized_note = personalized_note.replace("{company}", company)
                        if "{title}" in personalized_note and title:
                            personalized_note = personalized_note.replace("{title}", title)

                        print(f"Generated personalized note from template: {personalized_note}")

                    # Send connection request, passing the AI notes flag
                    result = connector.send_connection_request(linkedin_url, personalized_note, use_ai_notes)

                    # Update status in DataFrame based on result
                    df.at[j, "Connection Status"] = result["status"]
                    if result["success"]:
                        df.at[j, "Connection Date"] = current_date
                        if result["status"] == "request_sent":
                            requests_sent += 1
                            # Save the note used (if any)
                            if personalized_note:
                                df.at[j, "Connection Note"] = personalized_note
                        print(f"✅ Row {j + 1}: {result['status']}")
                        consecutive_errors = 0  # Reset consecutive errors counter
                    else:
                        requests_failed += 1
                        print(f"❌ Row {j + 1}: {result['status']}")

                        if "error" in result["status"].lower():
                            consecutive_errors += 1

                    # Save progress after each profile
                    df.to_csv(output_file, index=False)
                    print(f"Progress saved to {output_file}")

                    # Add random delay between profiles for safety
                    if j < batch_end - 1 and requests_sent < max_requests:
                        delay = random.uniform(45, 75)  # 45-75 seconds between profiles
                        print(f"Adding delay of {delay:.2f} seconds before next profile...")
                        time.sleep(delay)

                except Exception as e:
                    print(f"❌ Error processing row {j + 1}: {str(e)}")
                    df.at[j, "Connection Status"] = f"Error: {str(e)}"
                    consecutive_errors += 1
                    # Save on error to preserve progress
                    df.to_csv(output_file, index=False)

            # After each batch, take a longer break
            if batch_end < total_rows and requests_sent < max_requests:
                batch_delay = random.uniform(300, 600)  # 5-10 minutes between batches
                print(f"\nTaking a longer break for {batch_delay / 60:.2f} minutes between batches...")
                time.sleep(batch_delay)

            # Save progress after each batch
            print(f"\nSaving progress to {output_file}...")
            df.to_csv(output_file, index=False)
            processed_rows += batch_end - i
            completion_percentage = processed_rows / total_rows * 100
            print(f"Progress: {processed_rows}/{total_rows} rows processed ({completion_percentage:.2f}%)")
            print(f"Connection requests: {requests_sent} sent, {requests_failed} failed")

        return df

    except Exception as e:
        print(f"❌ Error processing CSV: {str(e)}")
        return None


def main():
    """Main function to run the LinkedIn connection automation"""
    print("\n" + "=" * 70)
    print("LinkedIn Connection Automation Tool")
    print("=" * 70 + "\n")

    # Debug information
    print(f"Python version: {platform.python_version()}")
    print(f"Current working directory: {os.getcwd()}")

    # Check for LinkedIn credentials
    if os.getenv("LINKEDIN_EMAIL") and os.getenv("LINKEDIN_PASSWORD"):
        print("✅ LinkedIn credentials found")
    else:
        print("❌ LinkedIn credentials not found - check your .env file")

    # Load configuration from environment variables
    linkedin_email = os.getenv("LINKEDIN_EMAIL")
    linkedin_password = os.getenv("LINKEDIN_PASSWORD")
    input_file = os.getenv("INPUT_CSV", "input.csv")
    output_file = os.getenv("OUTPUT_CSV", "connection_status.csv")
    batch_size = int(os.getenv("BATCH_SIZE", "5"))
    start_row = int(os.getenv("START_ROW", "0"))
    max_requests = int(os.getenv("MAX_REQUESTS", "20"))  # Reduced default to 20

    # Add more safety-focused parameters with lower defaults
    daily_limit_enforced = os.getenv("ENFORCE_DAILY_LIMIT", "True").lower() == "true"
    weekly_limit = int(os.getenv("WEEKLY_LIMIT", "100"))

    # Add option for AI-generated notes
    use_ai_notes = os.getenv("USE_AI_NOTES", "False").lower() == "true"

    # Check for OpenAI API key if AI notes are enabled
    if use_ai_notes:
        if os.getenv("OPENAI_API_KEY"):
            print("✅ OpenAI API key found")
        else:
            print("❌ OpenAI API key not found but AI notes are enabled.")
            print("Please set OPENAI_API_KEY in your .env file or disable AI notes.")
            return

    # Template for personalized notes
    personalized_note_template = os.getenv("PERSONALIZED_NOTE",
                                           "Hi {first_name}, I noticed your profile and would love to connect. I'm currently working with AI applications in sports and thought connecting would be valuable.")

    # Load or create the tracking file for weekly limits
    weekly_tracking_file = "linkedin_weekly_requests.json"
    weekly_data = {}

    try:
        if os.path.exists(weekly_tracking_file):
            with open(weekly_tracking_file, 'r') as f:
                weekly_data = json.load(f)
    except Exception as e:
        print(f"Could not load weekly tracking data: {str(e)}")

    # Calculate current week number
    current_date = datetime.now()
    current_week = f"{current_date.year}-{current_date.isocalendar()[1]}"  # Year-WeekNumber format

    # Initialize current week if not exists
    if current_week not in weekly_data:
        weekly_data[current_week] = 0

    # Check if weekly limit would be exceeded
    remaining_weekly = weekly_limit - weekly_data[current_week]
    if remaining_weekly <= 0:
        print(f"⚠️ Weekly connection request limit ({weekly_limit}) reached for week {current_week}")
        print("Please try again next week or adjust the WEEKLY_LIMIT in your .env file.")
        return

    # Adjust max_requests to respect remaining weekly limit
    if remaining_weekly < max_requests:
        print(f"Adjusting max requests from {max_requests} to {remaining_weekly} to respect weekly limit")
        max_requests = remaining_weekly

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
    print(f"- Maximum Requests: {max_requests}")
    print(f"- Weekly Limit: {weekly_limit} (Used: {weekly_data[current_week]}, Remaining: {remaining_weekly})")
    print(f"- Using AI-generated notes: {use_ai_notes}")

    # Prompt user to confirm
    try:
        confirmation = input("\nReady to begin sending LinkedIn connection requests? (y/n): ")
        if confirmation.lower() != 'y':
            print("Operation cancelled by user.")
            return
    except:
        # If running in an environment without input, proceed anyway
        pass

    try:
        # Initialize the LinkedIn connector
        print("\nInitializing LinkedIn connector...")
        connector = LinkedInConnector(linkedin_email, linkedin_password)

        # Process the CSV file and send connection requests
        print("\nBeginning connection request process...")
        df = process_connections(
            input_file,
            output_file,
            connector,
            batch_size,
            start_row,
            max_requests,
            personalized_note_template if not use_ai_notes else None,
            use_ai_notes
        )

        # Summarize results
        if df is not None:
            # Count statuses
            status_counts = df["Connection Status"].value_counts()
            print("\nConnection request summary:")
            for status, count in status_counts.items():
                print(f"- {status}: {count}")

            # After processing connections, update the weekly tracking data
            new_requests = status_counts.get("request_sent", 0)
            weekly_data[current_week] += new_requests

            # Save updated tracking data
            try:
                with open(weekly_tracking_file, 'w') as f:
                    json.dump(weekly_data, f)
                print(
                    f"Updated weekly tracking data: {new_requests} new requests, total {weekly_data[current_week]} for week {current_week}")
            except Exception as e:
                print(f"Could not save weekly tracking data: {str(e)}")

            print(f"\n✅ Processing complete. Results saved to {output_file}")
        else:
            print("\n❌ Processing failed. Check the error messages above.")

    except Exception as e:
        print(f"\n❌ An error occurred: {str(e)}")

    finally:
        # Close the connector to release resources
        if 'connector' in locals():
            connector.close()

    print("\n" + "=" * 70)
    print("LinkedIn Connection Automation Finished")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()