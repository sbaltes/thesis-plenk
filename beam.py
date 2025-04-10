import apache_beam as beam
from xml.etree import ElementTree as ET
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive.interactive_beam import options


def parse_xml_file(file_path):
    """
    Parse an XML file and save ID, CommentCount and ViewCount attributes of row elements in a dictionary.
    """
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()

        print(f"Root element: {root.tag}")
        print("Extracting Id and ViewCount from row elements...\n")

        # Dictionary to store Id and ViewCount
        row_data = {}
        # Find all row elements (at any level)
        row_count = 0
        for row in root.findall(".//row"):
            row_count += 1

            # Check if Id, CommentCount and ViewCount attributes exist
            if 'Id' in row.attrib and 'ViewCount' in row.attrib and 'CommentCount' in row.attrib:
                row_id = row.attrib['Id']
                view_count = row.attrib['ViewCount']
                comment_count = row.attrib['CommentCount']

                # Try to convert ViewCount and CommentCount to integer if possible
                try:
                    view_count = int(view_count)
                    comment_count = int(comment_count)
                except ValueError:
                    # Keep as string if conversion fails
                    pass

                # Store in dictionary
                row_data[row_id] = {view_count, comment_count}

        if row_count == 0:
            print("No row elements found in the XML file.")
        elif not row_data:
            print(f"Found {row_count} row elements, but none had both Id and ViewCount attributes.")
        else:
            print(f"Found {row_count} row elements, extracted {len(row_data)} with Id and ViewCount attributes.")

        return row_data

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return {}
    except ET.ParseError as e:
        print(f"Error: Failed to parse the XML file: {e}")
        return {}
    except Exception as e:
        print(f"Error: An unexpected error occurred: {e}")
        return {}


def compare_view_count(old_viewCount, new_ViewCount):
    data_difference = {}
    for row_id in new_ViewCount:
        if row_id in old_viewCount:
            data_difference[row_id] = new_ViewCount[row_id] - old_viewCount[row_id]
        else:
            data_difference[row_id] = new_ViewCount[row_id]

    return data_difference



