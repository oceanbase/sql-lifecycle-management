# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


# Set the file types that are allowed to be uploaded
ALLOWED_EXTENSIONS = {'xml', 'log', 'txt'}


# Check if the file type is legal
def allowed_file(filename):
    # Determine whether the extension of the file is in the configuration item ALLOWED_EXTENSIONS
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS
