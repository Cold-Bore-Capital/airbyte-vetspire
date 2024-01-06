#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_vetspire_v2 import SourceVetspireV2

if __name__ == "__main__":
    source = SourceVetspireV2()
    launch(source, sys.argv[1:])
