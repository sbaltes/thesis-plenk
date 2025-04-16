import apache_beam as beam
from xml.etree import ElementTree as ET
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners.interactive.interactive_beam import options
import os
import glob


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

        # Dictionary to store ID and ViewCount
        row_data = {}
        # Find all row elements (at any level)
        row_count = 0
        for row in root.findall(".//row"):
            row_count += 1

            # Check if ID and ViewCount attributes exist
            if 'Id' in row.attrib and 'ViewCount' in row.attrib and 'CommentCount' in row.attrib:
                row_id = row.attrib['Id']
                view_count = row.attrib['ViewCount']


                # Try to convert ViewCount to integer if possible
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
            print(f"Found {row_count} row elements in {file_path}, extracted {len(row_data)} with Id and ViewCount attributes.")

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


def compare_view_count(old_viewcount, new_viewcount):
    data_difference = {}
    for row_id in new_viewcount:
        if row_id in old_viewcount:
            data_difference[row_id] = new_viewcount[row_id] - old_viewcount[row_id]
        else:
            data_difference[row_id] = new_viewcount[row_id]

    return data_difference



def run():
    #get all xml-files in a given dir (here just the same folder but is changeable with a setup-file later
    current_dir = os.path.dirname(os.path.abspath(__file__))
    xml_files = glob.glob(os.path.join(current_dir, "*.xml"))


    options = PipelineOptions()
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DirectRunner'

    with beam.Pipeline(options=options) as pipeline:
        (pipeline
        | beam.Create(xml_files)
        | beam.Map(parse_xml_file)
        | beam.Map(print)
        )



if __name__ == "__main__":
    run()