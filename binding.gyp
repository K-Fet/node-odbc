{
  "targets": [
    {
      "target_name": "odbc_bindings",
      "sources": [
        "src/main.cpp",
        "src/utils.cpp",
        "src/deferred_async_worker.cpp",
        "src/odbc.cpp",
        "src/odbc_connection.cpp",
        "src/odbc_statement.cpp",
        "src/odbc_result.cpp"
      ],
      "cflags": [
        "-Wall",
        "-Wextra",
        "-Wno-unused-parameter"
      ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")"
      ],
      "defines": [
        "NAPI_DISABLE_CPP_EXCEPTIONS"
      ],
      "conditions": [
        [
          "OS == \"linux\"",
          {
            "libraries": [
              "-lodbc"
            ],
            "cflags": [
              "-g"
            ]
          }
        ],
        [
          "OS == \"mac\"",
          {
            "include_dirs": [
              "/usr/local/include"
            ],
            "libraries": [
              "-L/usr/local/lib",
              "-lodbc"
            ]
          }
        ],
        [
          "OS==\"win\"",
          {
            "sources": [
              "src/strptime.c"
            ],
            "libraries": [
              "-lodbccp32.lib"
            ]
          }
        ],
        [
          "OS==\"aix\"",
          {
            "variables": {
              "os_name": "<!(uname -s)"
            },
            "conditions": [
              [
                "\"<(os_name)\"==\"OS400\"",
                {
                  "ldflags": [
                    "-Wl,-brtl,-bnoquiet,-blibpath:/QOpenSys/pkgs/lib,-lodbc"
                  ],
                  "cflags": [
                    "-std=c++0x",
                    "-Wall",
                    "-Wextra",
                    "-Wno-unused-parameter",
                    "-I/QOpenSys/usr/include",
                    "-I/QOpenSys/pkgs/include"
                  ]
                }
              ]
            ]
          }
        ]
      ]
    }
  ]
}
