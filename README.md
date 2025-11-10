# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/pylhc/submitter/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                       |    Stmts |     Miss |   Cover |   Missing |
|----------------------------------------------------------- | -------: | -------: | ------: | --------: |
| pylhc\_submitter/\_\_init\_\_.py                           |        8 |        0 |    100% |           |
| pylhc\_submitter/autosix.py                                |       70 |       18 |     74% |351-363, 375, 384-393, 417-418 |
| pylhc\_submitter/constants/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| pylhc\_submitter/constants/autosix.py                      |      125 |       10 |     92% |98, 106, 120, 123, 126, 197, 201, 228, 232, 236 |
| pylhc\_submitter/constants/external\_paths.py              |        8 |        0 |    100% |           |
| pylhc\_submitter/constants/general.py                      |        3 |        0 |    100% |           |
| pylhc\_submitter/constants/htcondor.py                     |        8 |        0 |    100% |           |
| pylhc\_submitter/constants/job\_submitter.py               |       13 |        0 |    100% |           |
| pylhc\_submitter/job\_submitter.py                         |       89 |        9 |     90% |365, 371, 399-407, 427, 434-435 |
| pylhc\_submitter/sixdesk\_tools/\_\_init\_\_.py            |        0 |        0 |    100% |           |
| pylhc\_submitter/sixdesk\_tools/create\_workspace.py       |      116 |       26 |     78% |94-95, 102-116, 135, 149-150, 166-175, 197, 232, 237 |
| pylhc\_submitter/sixdesk\_tools/extract\_data\_from\_db.py |       24 |       11 |     54% |46-61, 71-73, 84-87 |
| pylhc\_submitter/sixdesk\_tools/post\_process\_da.py       |      142 |       50 |     65% |73-76, 89-97, 112-153, 168-173, 234, 237, 287, 297-300, 340 |
| pylhc\_submitter/sixdesk\_tools/stages.py                  |      181 |       46 |     75% |86-87, 92, 96-99, 102-105, 108-111, 114-117, 120-123, 126, 143, 173, 179, 182, 185, 195-196, 206, 243-247, 253, 309-310, 338, 358-368, 385, 403, 424, 452 |
| pylhc\_submitter/sixdesk\_tools/submit.py                  |       66 |       49 |     26% |52-68, 80-98, 110-127, 138-145, 157-167 |
| pylhc\_submitter/sixdesk\_tools/troubleshooting.py         |      139 |      139 |      0% |     8-255 |
| pylhc\_submitter/sixdesk\_tools/utils.py                   |       52 |       37 |     29% |26-31, 46-60, 68-108 |
| pylhc\_submitter/submitter/\_\_init\_\_.py                 |        0 |        0 |    100% |           |
| pylhc\_submitter/submitter/htc\_utils.py                   |      106 |       19 |     82% |98-104, 116-127, 223, 268, 309, 336, 344 |
| pylhc\_submitter/submitter/iotools.py                      |      138 |       13 |     91% |94, 97, 272-282, 320-321, 330 |
| pylhc\_submitter/submitter/mask.py                         |       46 |        4 |     91% |111-112, 117, 121 |
| pylhc\_submitter/submitter/runners.py                      |       51 |       12 |     76% |68-69, 76-78, 103-104, 120-131 |
| pylhc\_submitter/utils/\_\_init\_\_.py                     |        0 |        0 |    100% |           |
| pylhc\_submitter/utils/environment.py                      |        6 |        0 |    100% |           |
| pylhc\_submitter/utils/iotools.py                          |       54 |        4 |     93% |69-70, 72, 84 |
| pylhc\_submitter/utils/logging\_tools.py                   |        5 |        1 |     80% |        16 |
|                                                  **TOTAL** | **1450** |  **448** | **69%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/pylhc/submitter/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/pylhc/submitter/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pylhc/submitter/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/pylhc/submitter/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fpylhc%2Fsubmitter%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/pylhc/submitter/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.