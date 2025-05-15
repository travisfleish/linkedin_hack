import pandas as pd
import os
import openai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY")


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


def main():
    # Load the CSV file
    input_csv = os.getenv("INPUT_CSV", "input.csv")
    output_csv = os.getenv("OUTPUT_CSV", "leads_with_summaries_fixed.csv")

    print(f"Loading CSV from: {input_csv}")
    df = pd.read_csv(input_csv)

    # Confirm LinkedIn Profile column exists
    if "LinkedIn Profile" not in df.columns:
        print("Error: LinkedIn Profile column not found in the CSV")
        return

    # Initialize or reset LinkedIn Summary column
    df["LinkedIn Summary"] = ""

    # Count profiles to process
    profiles_to_process = df[df["LinkedIn Profile"].notna() & (df["LinkedIn Profile"] != "")]
    count = len(profiles_to_process)
    print(f"Found {count} LinkedIn profiles to process")

    # Ask for confirmation
    max_profiles = input(f"How many profiles do you want to process (max {count})? ")
    try:
        max_profiles = int(max_profiles)
        if max_profiles <= 0:
            print("Exiting without processing any profiles")
            return
        max_profiles = min(max_profiles, count)
    except:
        max_profiles = 5  # Default to 5 if input is invalid
        print(f"Invalid input. Processing first {max_profiles} profiles only")

    # Process profiles
    processed = 0
    for i, row in profiles_to_process.iterrows():
        if processed >= max_profiles:
            break

        url = row["LinkedIn Profile"]
        print(f"\nProcessing profile {processed + 1}/{max_profiles}: {url}")

        # Create a simple profile data from existing row information
        profile_data = {
            "name": f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip(),
            "title": row.get('Job Title', ''),
            "company": row.get('Company Name', ''),
            "location": row.get('Location', '')
        }

        # Only keep non-empty values
        profile_data = {k: v for k, v in profile_data.items() if v}

        if profile_data:
            summary = generate_summary(profile_data)
            if summary:
                # Save the summary directly to the dataframe
                df.at[i, "LinkedIn Summary"] = summary
                print(f"✅ Summary saved for row {i + 1}")

                # Save after each profile to avoid losing progress
                df.to_csv(output_csv, index=False)
                print(f"Progress saved to {output_csv}")

                processed += 1
        else:
            print(f"⚠️ No profile data found for {url}")

    # Final save
    df.to_csv(output_csv, index=False)
    print(f"\nProcessing complete. {processed} summaries generated and saved to {output_csv}")

    # Show a sample of the results
    if processed > 0:
        print("\nSample of generated summaries:")
        summaries = df[df["LinkedIn Summary"].notna() & (df["LinkedIn Summary"] != "")]
        for i in range(min(3, len(summaries))):
            row = summaries.iloc[i]
            name = f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
            summary = row.get('LinkedIn Summary', '')
            print(f"\nProfile: {name}")
            print(f"Summary: {summary}")


if __name__ == "__main__":
    main()