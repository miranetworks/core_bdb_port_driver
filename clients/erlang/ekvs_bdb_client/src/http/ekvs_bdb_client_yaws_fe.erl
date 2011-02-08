% $Id$
-module(ekvs_bdb_client_yaws_fe).

-export([
        start/0
        ]).

-include_lib("yaws/include/yaws_api.hrl").
-include_lib("yaws/include/yaws.hrl").

%Interface functions
start() ->

    StartWWW = pt_util_app:get_app_param(mira_ekvs_bdb_client, start_www, false),

    case StartWWW of
    true ->

    	application:set_env(yaws, embedded, true),
	    application:start(yaws),
	
	    GC = yaws_config:make_default_gconf(false, undefined),

	    GC1 = GC#gconf{tmpdir="./priv/"},

        Interface = pt_util_app:get_app_param(mira_ekvs_bdb_client, http_ext_if, {127,0,0,1}),

        Port      = pt_util_app:get_app_param(mira_ekvs_bdb_client, http_ext_port, 20090),

	    OrigSC = #sconf {
                        port       = Port,
			            servername = "mira_ekvs_bdb_client",
                        listen     = Interface,
                        docroot    = "./priv/www/",
                        appmods    =    [  
                                        {"set",         ekvs_bdb_client_yaws_fe_set_handler},
                                        {"get",         ekvs_bdb_client_yaws_fe_get_handler},
                                        {"remove",      ekvs_bdb_client_yaws_fe_remove_handler},
                                        {"count",       ekvs_bdb_client_yaws_fe_count_handler},
                                        {"bulk_get",    ekvs_bdb_client_yaws_fe_bulk_get_handler},

                                        {"nagios", ekvs_bdb_client_yaws_fe_nagios_req_handler}
                                        ] 
		                },

        SC = ?sc_set_access_log(OrigSC, false),

        yaws_api:setconf(GC1, [[SC]]);

    _ ->
        not_started
    end.

%EOF
