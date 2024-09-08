import pandas as pd
import random
import string

# Function to generate random strings
def random_string(length=5):
    return ''.join(random.choices(string.ascii_uppercase, k=length))

# Function to generate random flags (True/False)
def random_flag():
    return random.choice([True, False])

# Generate source_data with potential duplicate ids/names
def generate_data(size):
    ids = [random.randint(1, 50) for _ in range(size)]  # Randomly pick ids from 1 to 50, allowing duplicates
    names = [random_string() for _ in range(size)]
    flags = [random_flag() for _ in range(size)]
    return pd.DataFrame({'id': ids, 'name': names, 'flag': flags})

# Generate source and destination datasets
source_df = generate_data(100)  # Source has 100 records
destination_df = generate_data(200)  # Destination has 200 records

# Ensure the data types of source and destination columns match
for column in source_df.columns:
    if column in destination_df.columns:
        destination_df[column] = destination_df[column].astype(source_df[column].dtype)

# Strip whitespace from name columns (if necessary)
source_df['name'] = source_df['name'].str.strip()
destination_df['name'] = destination_df['name'].str.strip()

# Introduce random matching records between source and destination
def introduce_matches(source_df, destination_df, num_matches=50):
    for i in range(num_matches):
        # Pick a random index from the destination and update it to match a random source row
        dest_idx = random.randint(0, len(destination_df) - 1)
        src_idx = random.randint(0, len(source_df) - 1)
        
        destination_df.at[dest_idx, 'id'] = source_df.at[src_idx, 'id']
        destination_df.at[dest_idx, 'name'] = source_df.at[src_idx, 'name']
        destination_df.at[dest_idx, 'flag'] = source_df.at[src_idx, 'flag']

introduce_matches(source_df, destination_df, num_matches=50)  # Introduce 50 random matches

# Function to compare tables and highlight differences
def compare_tables(source_df, destination_df, compare_by):
    unmatched_destinations = destination_df.copy()
    matched_source = []
    matched_dest = []

    source_diff = pd.DataFrame(False, index=source_df.index, columns=source_df.columns)
    dest_diff = pd.DataFrame(False, index=destination_df.index, columns=destination_df.columns)

    # Iterate over the source DataFrame and try to match each row to a destination row
    for src_idx, source_row in source_df.iterrows():
        # Look for rows in destination with matching `id` (or other `compare_by` fields)
        matching_dest_rows = unmatched_destinations[
            (unmatched_destinations[compare_by] == source_row[compare_by]).all(axis=1)
        ]

        # Check if a matching row exists
        if not matching_dest_rows.empty:
            found_match = False
            # Compare each matching destination row with the source row for full match
            for dest_idx, dest_row in matching_dest_rows.iterrows():
                match = True
                for col in source_df.columns:
                    if source_row[col] != dest_row[col]:
                        # If any column doesn't match, mark them as different
                        source_diff.at[src_idx, col] = True
                        dest_diff.at[dest_idx, col] = False  # Mark as not different for the destination row
                        match = False
                
                if match:
                    # Exact match found, mark both as matched
                    matched_source.append(src_idx)
                    matched_dest.append(dest_idx)
                    unmatched_destinations.drop(dest_idx, inplace=True)
                    found_match = True
                    break  # Stop checking further if exact match is found
            
            # If no exact match, mark the source row as partially matched
            if not found_match:
                for col in source_df.columns:
                    if col != compare_by[0]:  # Exclude the compare_by column from marking as different
                        dest_diff.at[dest_idx, col] = True  # Highlight destination row differences
            
        else:
            # No match at all, mark the source row as different
            source_diff.loc[src_idx] = True

    # Mark the remaining unmatched destination rows as different
    for dest_idx in unmatched_destinations.index:
        dest_diff.loc[dest_idx] = True

    return matched_source, matched_dest, unmatched_destinations.index.tolist(), source_diff, dest_diff

# Function to display and highlight differences
def highlight_differences(source_df, destination_df, matched_source, matched_dest, unmatched_dest, source_diff, dest_diff):
    def highlight_row_source(row):
        # Highlight the source rows: green if no difference, yellow if difference
        colors = ['background-color: green' if not diff else 'background-color: yellow' for diff in source_diff.loc[row.name]]
        return colors

    def highlight_row_destination(row):
        # Highlight the destination rows: green if no difference, yellow if difference
        colors = ['background-color: green' if not diff else 'background-color: yellow' for diff in dest_diff.loc[row.name]]
        return colors

    # Reorder the source and destination DataFrames
    source_ordered = pd.concat([
        source_df.loc[matched_source],
        source_df[~source_df.index.isin(matched_source)]
    ])
    
    destination_ordered = pd.concat([
        destination_df.loc[matched_dest],
        destination_df.loc[unmatched_dest]
    ])
    
    # Style the reordered DataFrames
    source_styled = source_ordered.style.apply(highlight_row_source, axis=1)
    destination_styled = destination_ordered.style.apply(highlight_row_destination, axis=1)

    # Save to HTML file
    with open("html/output.html", "w") as f:
        f.write(f"""
        <html>
        <head><style>table {{border-collapse: collapse;}} td, th {{padding: 8px; border: 1px solid black;}}</style></head>
        <body>
            <div style="display: flex;">
                <div style="flex: 1; padding-right: 20px;">
                    <h3>Source Data (Matches on Top)</h3>
                    {source_styled.to_html()}
                </div>
                <div style="flex: 1;">
                    <h3>Destination Data (Matches on Top)</h3>
                    {destination_styled.to_html()}
                </div>
            </div>
        </body>
        </html>
        """)
    print("Styled output saved as 'output.html'. Open this file in a browser to view the highlighted differences.")

# Example usage
compare_by = ['name']  # Compare by 'id' only, but match all other columns as well
matched_source, matched_dest, unmatched_dest, source_diff, dest_diff = compare_tables(source_df, destination_df, compare_by)

# Display both tables side by side with highlighted differences
highlight_differences(source_df, destination_df, matched_source, matched_dest, unmatched_dest, source_diff, dest_diff)
