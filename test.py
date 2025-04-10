import xml.etree.ElementTree as ET


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

        # Dictionary to store Id and its ViewCount
        row_data = {}
        # Find all row elements (at any level)
        row_count = 0
        for row in root.findall(".//row"):
            row_count += 1

            # Check if Id and ViewCount attributes exist
            if 'Id' in row.attrib and 'ViewCount' in row.attrib:
                row_id = row.attrib['Id']
                view_count = row.attrib['ViewCount']

                try:
                    row_id = int(row_id)
                except ValueError:
                    pass
                # Try to convert ViewCount  to integer if possible
                try:
                    view_count = int(view_count)
                except ValueError:
                    # Keep as string if conversion fails
                    pass

                # Store in dictionary
                row_data[row_id] = view_count

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

if __name__ == "__main__":
        old_dump = parse_xml_file("Posts30_09_2024.xml")
        new_dump = parse_xml_file("Posts31_12_2024.xml")

        result = compare_view_count(old_dump, new_dump)
        # Print the dictionary
        print("\nDictionary of Id and ViewCount-differential")
        print(result)
        print(len(result))

        # Print in a more readable format if there are many entries
        if len(result) > 10:
            print("\nSample of first 10 entries:")
            for i, (key, value) in enumerate(result.items()):
                print(f"{key}: {value}")
                if i >= 9:
                    break

