


import os

# List of names for which copies of the file will be made
names = [
    'dab012',
    'dab010',
    'dfw012',
    'nsh013',
    'nsh011',
    'nsh_012',
    'nsh_010',
    'gvl010',
    'dab011',
    'dfw010',
    'nsh012',
    'nsh010',
    'knx011',
    'gvl011'
]

# Path to the original file
original_file = "../encounters.json"
output_directory = "../"

# Read the content of the original file
with open(original_file, 'r') as file:
    original_content = file.read()

# Create copies of the file for each name
for name in names:
    # Generate the new filename
    new_filename = os.path.join(output_directory, f"encounters_{name}.json")

    # Write the original content to the new file
    with open(new_filename, 'w') as new_file:
        new_file.write(original_content)

print("Files created successfully!")
