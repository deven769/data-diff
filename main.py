import pandas as pd
import sqlalchemy
from sqlalchemy import text
import html
import time

class OptimizedDBComparisonTool:
    def __init__(self, db_connection_string):
        self.engine = sqlalchemy.create_engine(db_connection_string)

    def fetch_data(self, table_name, limit):
        query = text(f"SELECT * FROM {table_name} LIMIT {limit}")
        return pd.read_sql(query, self.engine)

    def compare_tables(self, source_table, dest_table, compare_by, s_limit, d_limit):
        source_df = self.fetch_data(source_table, s_limit)
        dest_df = self.fetch_data(dest_table, d_limit)

        # Ensure compare_by columns exist in both dataframes
        assert all(col in source_df.columns for col in compare_by), "Compare by column(s) not found in source table"
        assert all(col in dest_df.columns for col in compare_by), "Compare by column(s) not found in destination table"
        
        # Group by the compare_by columns for both dataframes
        source_grouped = source_df.groupby(compare_by).apply(lambda x: x.reset_index(drop=True))
        dest_grouped = dest_df.groupby(compare_by).apply(lambda x: x.reset_index(drop=True))

        # Set of all unique keys for comparison
        all_keys = set(source_grouped.index).union(set(dest_grouped.index))
        # print(all_keys)
        # import pdb; pdb.set_trace()

        comparison_result = []
        for key in sorted(all_keys):
            # Fetch all rows for the given key in both source and destination
            source_rows = source_grouped[source_grouped.index == key]
            dest_rows = dest_grouped[dest_grouped.index == key]

            max_len = max(len(source_rows), len(dest_rows))
            
            for i in range(max_len):
                # Fetch rows or empty rows if one table has fewer rows
                source_row = source_rows.iloc[i] if i < len(source_rows) else pd.Series(dtype='object')
                dest_row = dest_rows.iloc[i] if i < len(dest_rows) else pd.Series(dtype='object')

                # Compare the rows
                if source_row.empty and not dest_row.empty:
                    # Row only in destination (green)
                    comparison_result.append(('', '', 'lightgreen', dest_row.tolist()))
                elif not source_row.empty and dest_row.empty:
                    # Row only in source (red)
                    comparison_result.append((key, source_row.tolist(), 'lightcoral', ''))
                else:
                    # Row in both, compare column by column
                    row_result = []
                    for source_val, dest_val in zip(source_row, dest_row):
                        if source_val == dest_val:
                            # Columns match (white)
                            row_result.append(('white', source_val, dest_val))
                        else:
                            # Columns differ (blue)
                            row_result.append(('lightblue', source_val, dest_val))
                    comparison_result.append((key, row_result))
        print(comparison_result)

        return comparison_result

    def generate_html(self, comparison_result):
        html_content = """
        <html>
        <head>
            <style>
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid black; padding: 8px; text-align: left; }
                th { background-color: #f2f2f2; }
                .cell { padding: 4px; }
            </style>
        </head>
        <body>
            <table>
                <tr>
                    <th>Source Index</th>
                    <th>Source Data</th>
                    <th>Destination Index</th>
                    <th>Destination Data</th>
                </tr>
        """

        for idx, result in enumerate(comparison_result, start=1):
            if len(result) == 4:
                # Row only exists in source or destination
                source_idx, source_data, color, dest_data = result
                html_content += f"<tr style='background-color: {color};'>"
                if source_data:
                    html_content += f"<td>{source_idx}</td><td>{' '.join(map(str, source_data))}</td>"
                else:
                    html_content += "<td></td><td></td>"
                
                if dest_data:
                    html_content += f"<td>{idx}</td><td>{' '.join(map(str, dest_data))}</td>"
                else:
                    html_content += "<td></td><td></td>"
            else:
                # Partial match or exact match, render column-wise
                source_idx, row_result = result
                html_content += "<tr>"

                # Source columns
                html_content += f"<td>{source_idx}</td><td>"
                for color, source_val, _ in row_result:
                    html_content += f"<span class='cell' style='background-color: {color};'>{html.escape(str(source_val))}</span> "
                html_content += "</td>"

                # Destination columns
                html_content += f"<td>{idx}</td><td>"
                for color, _, dest_val in row_result:
                    html_content += f"<span class='cell' style='background-color: {color};'>{html.escape(str(dest_val))}</span> "
                html_content += "</td></tr>"

        html_content += """
            </table>
        </body>
        </html>
        """

        with open("html/comparison_result.html", "w") as f:
            f.write(html_content)

        print("Comparison result saved as 'comparison_result.html'")

# Example usage
if __name__ == "__main__":
    db_connection_string = "postgresql://deven:deven8000@localhost:5432/datadiff"
    tool = OptimizedDBComparisonTool(db_connection_string)
    
    source_table = "source"
    dest_table = "destination"
    
    # Dynamic "compare by" column
    compare_by = ['id']

    s_limit = 5000
    d_limit = 5000

    now = time.time()
    comparison_result = tool.compare_tables(source_table, dest_table, compare_by, s_limit, d_limit)
    tool.generate_html(comparison_result)
    end = time.time()
    execution_time = end - now
    print(f"Execution time: {execution_time:.2f} seconds")
