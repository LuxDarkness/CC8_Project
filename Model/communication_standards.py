Request = """From:{}
To:{}
Name:{}
Size:{}
EOF
"""

Correct_Answer = """From:{}
To:{}
Name:{}
Data:{}
Frag:{}
Size:{}
EOF
"""

Error_Answer = """From:{}
To:{}
Msg:{}
EOF
"""

Routing_Client_Start = """From:{}
Type:HELLO
"""

Routing_Server_Greeting = """From:{}
Type:WELCOME
"""

Routing_Client_Send_Update = """From:{}
Type:DV
Len:{}
"""

Routing_Route_Msg = """{}:{}"""

Routing_Client_Keep_Alive = """From:{}
Type:KeepAlive
"""
