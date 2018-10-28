Search.setIndex({docnames:["columns","databases","events","expr","index","miscellaneous","store"],envversion:{"sphinx.domains.c":1,"sphinx.domains.changeset":1,"sphinx.domains.cpp":1,"sphinx.domains.javascript":1,"sphinx.domains.math":1,"sphinx.domains.python":1,"sphinx.domains.rst":1,"sphinx.domains.std":1,"sphinx.ext.viewcode":1,sphinx:54},filenames:["columns.rst","databases.rst","events.rst","expr.rst","index.rst","miscellaneous.rst","store.rst"],objects:{"storm.base":{Storm:[5,1,1,""]},"storm.cache":{Cache:[5,1,1,""],GenerationalCache:[5,1,1,""]},"storm.cache.Cache":{add:[5,2,1,""],clear:[5,2,1,""],get_cached:[5,2,1,""],remove:[5,2,1,""],set_size:[5,2,1,""]},"storm.cache.GenerationalCache":{add:[5,2,1,""],clear:[5,2,1,""],get_cached:[5,2,1,""],remove:[5,2,1,""],set_size:[5,2,1,""]},"storm.database":{Connection:[1,1,1,""],Database:[1,1,1,""],Result:[1,1,1,""],convert_param_marks:[1,5,1,""],create_database:[1,5,1,""],register_scheme:[1,5,1,""]},"storm.database.Connection":{begin:[1,2,1,""],block_access:[1,2,1,""],build_raw_cursor:[1,2,1,""],close:[1,2,1,""],commit:[1,2,1,""],compile:[1,3,1,""],execute:[1,2,1,""],is_disconnection_error:[1,2,1,""],param_mark:[1,3,1,""],prepare:[1,2,1,""],preset_primary_key:[1,2,1,""],raw_execute:[1,2,1,""],recover:[1,2,1,""],result_factory:[1,3,1,""],rollback:[1,2,1,""],to_database:[1,4,1,""],unblock_access:[1,2,1,""]},"storm.database.Database":{connect:[1,2,1,""],connection_factory:[1,3,1,""],get_uri:[1,2,1,""],raw_connect:[1,2,1,""]},"storm.database.Result":{close:[1,2,1,""],from_database:[1,4,1,""],get_all:[1,2,1,""],get_insert_identity:[1,2,1,""],get_one:[1,2,1,""],rowcount:[1,3,1,""],set_variable:[1,4,1,""]},"storm.databases":{postgres:[1,0,0,"-"],sqlite:[1,0,0,"-"]},"storm.databases.postgres":{Case:[1,1,1,""],JSON:[1,1,1,""],JSONElement:[1,1,1,""],JSONTextElement:[1,1,1,""],JSONVariable:[1,1,1,""],Postgres:[1,1,1,""],PostgresConnection:[1,1,1,""],PostgresResult:[1,1,1,""],PostgresTimeoutTracer:[1,1,1,""],Returning:[1,1,1,""],compile_case:[1,5,1,""],compile_currval:[1,5,1,""],compile_insert_postgres:[1,5,1,""],compile_like_postgres:[1,5,1,""],compile_list_variable:[1,5,1,""],compile_returning:[1,5,1,""],compile_sequence_postgres:[1,5,1,""],compile_set_expr_postgres:[1,5,1,""],compile_sql_token_postgres:[1,5,1,""],create_from_uri:[1,3,1,""],currval:[1,1,1,""],make_dsn:[1,5,1,""]},"storm.databases.postgres.JSON":{variable_class:[1,3,1,""]},"storm.databases.postgres.JSONElement":{oper:[1,3,1,""]},"storm.databases.postgres.JSONTextElement":{oper:[1,3,1,""]},"storm.databases.postgres.Postgres":{connection_factory:[1,3,1,""],raw_connect:[1,2,1,""]},"storm.databases.postgres.PostgresConnection":{compile:[1,3,1,""],execute:[1,2,1,""],is_disconnection_error:[1,2,1,""],param_mark:[1,3,1,""],result_factory:[1,3,1,""],to_database:[1,2,1,""]},"storm.databases.postgres.PostgresResult":{get_insert_identity:[1,2,1,""]},"storm.databases.postgres.PostgresTimeoutTracer":{connection_raw_execute_error:[1,2,1,""],set_statement_timeout:[1,2,1,""]},"storm.databases.postgres.currval":{name:[1,3,1,""]},"storm.databases.sqlite":{SQLite:[1,1,1,""],SQLiteConnection:[1,1,1,""],SQLiteResult:[1,1,1,""],compile_insert_sqlite:[1,5,1,""],compile_select_sqlite:[1,5,1,""],create_from_uri:[1,3,1,""]},"storm.databases.sqlite.SQLite":{connection_factory:[1,3,1,""],raw_connect:[1,2,1,""]},"storm.databases.sqlite.SQLiteConnection":{commit:[1,2,1,""],compile:[1,3,1,""],raw_execute:[1,2,1,""],result_factory:[1,3,1,""],rollback:[1,2,1,""],to_database:[1,4,1,""]},"storm.databases.sqlite.SQLiteResult":{from_database:[1,4,1,""],get_insert_identity:[1,2,1,""],set_variable:[1,4,1,""]},"storm.event":{EventSystem:[2,1,1,""]},"storm.event.EventSystem":{emit:[2,2,1,""],hook:[2,2,1,""],unhook:[2,2,1,""]},"storm.exceptions":{ClassInfoError:[5,6,1,""],ClosedError:[5,6,1,""],CompileError:[5,6,1,""],ConnectionBlockedError:[5,6,1,""],DataError:[5,6,1,""],DatabaseError:[5,6,1,""],DatabaseModuleError:[5,6,1,""],DisconnectionError:[5,6,1,""],Error:[5,6,1,""],ExprError:[5,6,1,""],FeatureError:[5,6,1,""],IntegrityError:[5,6,1,""],InterfaceError:[5,6,1,""],InternalError:[5,6,1,""],LostObjectError:[5,6,1,""],NoStoreError:[5,6,1,""],NoTableError:[5,6,1,""],NoneError:[5,6,1,""],NotFlushedError:[5,6,1,""],NotOneError:[5,6,1,""],NotSupportedError:[5,6,1,""],OperationalError:[5,6,1,""],OrderLoopError:[5,6,1,""],ProgrammingError:[5,6,1,""],PropertyPathError:[5,6,1,""],StoreError:[5,6,1,""],StormError:[5,6,1,""],TimeoutError:[5,6,1,""],URIError:[5,6,1,""],UnorderedError:[5,6,1,""],Warning:[5,6,1,""],WrongStoreError:[5,6,1,""],install_exceptions:[5,5,1,""]},"storm.expr":{Add:[3,1,1,""],Alias:[3,1,1,""],And:[3,1,1,""],Asc:[3,1,1,""],AutoTables:[3,1,1,""],Avg:[3,1,1,""],BinaryExpr:[3,1,1,""],BinaryOper:[3,1,1,""],Cast:[3,1,1,""],Coalesce:[3,1,1,""],Column:[3,1,1,""],Comparable:[3,1,1,""],ComparableExpr:[3,1,1,""],CompilePython:[3,1,1,""],CompoundExpr:[3,1,1,""],CompoundOper:[3,1,1,""],Context:[3,1,1,""],Count:[3,1,1,""],Delete:[3,1,1,""],Desc:[3,1,1,""],Distinct:[3,1,1,""],Div:[3,1,1,""],Eq:[3,1,1,""],Except:[3,1,1,""],Exists:[3,1,1,""],Expr:[3,1,1,""],FromExpr:[3,1,1,""],Func:[3,1,1,""],FuncExpr:[3,1,1,""],Ge:[3,1,1,""],Gt:[3,1,1,""],In:[3,1,1,""],Insert:[3,1,1,""],Intersect:[3,1,1,""],Join:[3,1,1,""],JoinExpr:[3,1,1,""],LShift:[3,1,1,""],Le:[3,1,1,""],LeftJoin:[3,1,1,""],Like:[3,1,1,""],Lower:[3,1,1,""],Lt:[3,1,1,""],Max:[3,1,1,""],Min:[3,1,1,""],Mod:[3,1,1,""],Mul:[3,1,1,""],NamedFunc:[3,1,1,""],NaturalJoin:[3,1,1,""],NaturalLeftJoin:[3,1,1,""],NaturalRightJoin:[3,1,1,""],Ne:[3,1,1,""],Neg:[3,1,1,""],NonAssocBinaryOper:[3,1,1,""],Not:[3,1,1,""],Or:[3,1,1,""],PrefixExpr:[3,1,1,""],RShift:[3,1,1,""],RightJoin:[3,1,1,""],Row:[3,1,1,""],SQL:[3,1,1,""],SQLRaw:[3,1,1,""],SQLToken:[3,1,1,""],Select:[3,1,1,""],Sequence:[3,1,1,""],SetExpr:[3,1,1,""],State:[3,1,1,""],Sub:[3,1,1,""],SuffixExpr:[3,1,1,""],Sum:[3,1,1,""],Table:[3,1,1,""],Union:[3,1,1,""],Update:[3,1,1,""],Upper:[3,1,1,""],build_tables:[3,5,1,""],compare_columns:[3,5,1,""],compile_alias:[3,5,1,""],compile_auto_tables:[3,5,1,""],compile_binary_oper:[3,5,1,""],compile_bool:[3,5,1,""],compile_cast:[3,5,1,""],compile_column:[3,5,1,""],compile_compound_oper:[3,5,1,""],compile_count:[3,5,1,""],compile_date:[3,5,1,""],compile_datetime:[3,5,1,""],compile_decimal:[3,5,1,""],compile_delete:[3,5,1,""],compile_distinct:[3,5,1,""],compile_eq:[3,5,1,""],compile_float:[3,5,1,""],compile_func:[3,5,1,""],compile_in:[3,5,1,""],compile_insert:[3,5,1,""],compile_int:[3,5,1,""],compile_join:[3,5,1,""],compile_like:[3,5,1,""],compile_ne:[3,5,1,""],compile_neg_expr:[3,5,1,""],compile_non_assoc_binary_oper:[3,5,1,""],compile_none:[3,5,1,""],compile_prefix_expr:[3,5,1,""],compile_python_bool_and_dates:[3,5,1,""],compile_python_builtin:[3,5,1,""],compile_python_column:[3,5,1,""],compile_python_sql_token:[3,5,1,""],compile_python_unsupported:[3,5,1,""],compile_python_variable:[3,5,1,""],compile_select:[3,5,1,""],compile_set_expr:[3,5,1,""],compile_sql:[3,5,1,""],compile_sql_token:[3,5,1,""],compile_str:[3,5,1,""],compile_suffix_expr:[3,5,1,""],compile_table:[3,5,1,""],compile_time:[3,5,1,""],compile_timedelta:[3,5,1,""],compile_unicode:[3,5,1,""],compile_update:[3,5,1,""],compile_variable:[3,5,1,""],has_tables:[3,5,1,""],is_safe_token:[3,5,1,""]},"storm.expr.Add":{oper:[3,3,1,""]},"storm.expr.Alias":{auto_counter:[3,3,1,""],expr:[3,3,1,""],name:[3,3,1,""]},"storm.expr.And":{oper:[3,3,1,""]},"storm.expr.Asc":{suffix:[3,3,1,""]},"storm.expr.AutoTables":{expr:[3,3,1,""],replace:[3,3,1,""],tables:[3,3,1,""]},"storm.expr.Avg":{name:[3,3,1,""]},"storm.expr.BinaryExpr":{expr1:[3,3,1,""],expr2:[3,3,1,""]},"storm.expr.BinaryOper":{oper:[3,3,1,""]},"storm.expr.Cast":{column:[3,3,1,""],name:[3,3,1,""],type:[3,3,1,""]},"storm.expr.Coalesce":{name:[3,3,1,""]},"storm.expr.Column":{compile_cache:[3,3,1,""],compile_id:[3,3,1,""],name:[3,3,1,""],primary:[3,3,1,""],table:[3,3,1,""],variable_factory:[3,3,1,""]},"storm.expr.Comparable":{contains_string:[3,2,1,""],endswith:[3,2,1,""],is_in:[3,2,1,""],like:[3,2,1,""],lower:[3,2,1,""],startswith:[3,2,1,""],upper:[3,2,1,""]},"storm.expr.CompilePython":{get_matcher:[3,2,1,""]},"storm.expr.CompoundExpr":{exprs:[3,3,1,""]},"storm.expr.CompoundOper":{oper:[3,3,1,""]},"storm.expr.Count":{column:[3,3,1,""],distinct:[3,3,1,""],name:[3,3,1,""]},"storm.expr.Delete":{default_table:[3,3,1,""],table:[3,3,1,""],where:[3,3,1,""]},"storm.expr.Desc":{suffix:[3,3,1,""]},"storm.expr.Distinct":{expr:[3,3,1,""]},"storm.expr.Div":{oper:[3,3,1,""]},"storm.expr.Eq":{oper:[3,3,1,""]},"storm.expr.Except":{oper:[3,3,1,""]},"storm.expr.Exists":{prefix:[3,3,1,""]},"storm.expr.Func":{args:[3,3,1,""],name:[3,3,1,""]},"storm.expr.FuncExpr":{name:[3,3,1,""]},"storm.expr.Ge":{oper:[3,3,1,""]},"storm.expr.Gt":{oper:[3,3,1,""]},"storm.expr.In":{oper:[3,3,1,""]},"storm.expr.Insert":{default_table:[3,3,1,""],map:[3,3,1,""],primary_columns:[3,3,1,""],primary_variables:[3,3,1,""],table:[3,3,1,""],values:[3,3,1,""]},"storm.expr.Intersect":{oper:[3,3,1,""]},"storm.expr.Join":{oper:[3,3,1,""]},"storm.expr.JoinExpr":{left:[3,3,1,""],on:[3,3,1,""],oper:[3,3,1,""],right:[3,3,1,""]},"storm.expr.LShift":{oper:[3,3,1,""]},"storm.expr.Le":{oper:[3,3,1,""]},"storm.expr.LeftJoin":{oper:[3,3,1,""]},"storm.expr.Like":{case_sensitive:[3,3,1,""],escape:[3,3,1,""],oper:[3,3,1,""]},"storm.expr.Lower":{name:[3,3,1,""]},"storm.expr.Lt":{oper:[3,3,1,""]},"storm.expr.Max":{name:[3,3,1,""]},"storm.expr.Min":{name:[3,3,1,""]},"storm.expr.Mod":{oper:[3,3,1,""]},"storm.expr.Mul":{oper:[3,3,1,""]},"storm.expr.NamedFunc":{args:[3,3,1,""]},"storm.expr.NaturalJoin":{oper:[3,3,1,""]},"storm.expr.NaturalLeftJoin":{oper:[3,3,1,""]},"storm.expr.NaturalRightJoin":{oper:[3,3,1,""]},"storm.expr.Ne":{oper:[3,3,1,""]},"storm.expr.Neg":{prefix:[3,3,1,""]},"storm.expr.NonAssocBinaryOper":{oper:[3,3,1,""]},"storm.expr.Not":{prefix:[3,3,1,""]},"storm.expr.Or":{oper:[3,3,1,""]},"storm.expr.PrefixExpr":{expr:[3,3,1,""],prefix:[3,3,1,""]},"storm.expr.RShift":{oper:[3,3,1,""]},"storm.expr.RightJoin":{oper:[3,3,1,""]},"storm.expr.Row":{name:[3,3,1,""]},"storm.expr.SQL":{expr:[3,3,1,""],params:[3,3,1,""],tables:[3,3,1,""]},"storm.expr.Select":{columns:[3,3,1,""],default_tables:[3,3,1,""],distinct:[3,3,1,""],group_by:[3,3,1,""],having:[3,3,1,""],limit:[3,3,1,""],offset:[3,3,1,""],order_by:[3,3,1,""],tables:[3,3,1,""],where:[3,3,1,""]},"storm.expr.Sequence":{name:[3,3,1,""]},"storm.expr.SetExpr":{all:[3,3,1,""],exprs:[3,3,1,""],limit:[3,3,1,""],offset:[3,3,1,""],oper:[3,3,1,""],order_by:[3,3,1,""]},"storm.expr.State":{pop:[3,2,1,""],push:[3,2,1,""]},"storm.expr.Sub":{oper:[3,3,1,""]},"storm.expr.SuffixExpr":{expr:[3,3,1,""],suffix:[3,3,1,""]},"storm.expr.Sum":{name:[3,3,1,""]},"storm.expr.Table":{compile_cache:[3,3,1,""],compile_id:[3,3,1,""],name:[3,3,1,""]},"storm.expr.Union":{oper:[3,3,1,""]},"storm.expr.Update":{default_table:[3,3,1,""],map:[3,3,1,""],primary_columns:[3,3,1,""],table:[3,3,1,""],where:[3,3,1,""]},"storm.expr.Upper":{name:[3,3,1,""]},"storm.info":{ClassAlias:[6,1,1,""],ClassInfo:[6,1,1,""],ObjectInfo:[6,1,1,""],get_cls_info:[6,5,1,""],get_obj_info:[6,5,1,""],set_obj_info:[6,5,1,""]},"storm.info.ClassAlias":{alias_count:[6,3,1,""]},"storm.info.ObjectInfo":{checkpoint:[6,2,1,""],cls_info:[6,3,1,""],event:[6,3,1,""],get_obj:[6,2,1,""],primary_vars:[6,3,1,""],set_obj:[6,2,1,""],variables:[6,3,1,""]},"storm.properties":{Bool:[0,1,1,""],Date:[0,1,1,""],DateTime:[0,1,1,""],Decimal:[0,1,1,""],Enum:[0,1,1,""],Float:[0,1,1,""],Int:[0,1,1,""],JSON:[0,1,1,""],List:[0,1,1,""],Property:[0,1,1,""],PropertyRegistry:[0,1,1,""],RawStr:[0,1,1,""],SimpleProperty:[0,1,1,""],Time:[0,1,1,""],TimeDelta:[0,1,1,""],UUID:[0,1,1,""],Unicode:[0,1,1,""]},"storm.properties.Bool":{variable_class:[0,3,1,""]},"storm.properties.Date":{variable_class:[0,3,1,""]},"storm.properties.DateTime":{variable_class:[0,3,1,""]},"storm.properties.Decimal":{variable_class:[0,3,1,""]},"storm.properties.Enum":{variable_class:[0,3,1,""]},"storm.properties.Float":{variable_class:[0,3,1,""]},"storm.properties.Int":{variable_class:[0,3,1,""]},"storm.properties.JSON":{variable_class:[0,3,1,""]},"storm.properties.List":{variable_class:[0,3,1,""]},"storm.properties.PropertyRegistry":{add_class:[0,2,1,""],add_property:[0,2,1,""],clear:[0,2,1,""],get:[0,2,1,""]},"storm.properties.RawStr":{variable_class:[0,3,1,""]},"storm.properties.SimpleProperty":{variable_class:[0,3,1,""]},"storm.properties.Time":{variable_class:[0,3,1,""]},"storm.properties.TimeDelta":{variable_class:[0,3,1,""]},"storm.properties.UUID":{variable_class:[0,3,1,""]},"storm.properties.Unicode":{variable_class:[0,3,1,""]},"storm.references":{Proxy:[0,1,1,""],Reference:[0,1,1,""],ReferenceSet:[0,1,1,""]},"storm.references.Proxy":{RemoteProp:[0,1,1,""],variable_factory:[0,3,1,""]},"storm.store":{EmptyResultSet:[6,1,1,""],Store:[6,1,1,""]},"storm.store.EmptyResultSet":{any:[6,2,1,""],avg:[6,2,1,""],cached:[6,2,1,""],config:[6,2,1,""],copy:[6,2,1,""],count:[6,2,1,""],difference:[6,2,1,""],find:[6,2,1,""],first:[6,2,1,""],get_select_expr:[6,2,1,""],group_by:[6,2,1,""],intersection:[6,2,1,""],is_empty:[6,2,1,""],last:[6,2,1,""],max:[6,2,1,""],min:[6,2,1,""],one:[6,2,1,""],order_by:[6,2,1,""],remove:[6,2,1,""],set:[6,2,1,""],sum:[6,2,1,""],union:[6,2,1,""],values:[6,2,1,""]},"storm.store.Store":{add:[6,2,1,""],add_flush_order:[6,2,1,""],autoreload:[6,2,1,""],begin:[6,2,1,""],block_access:[6,2,1,""],block_implicit_flushes:[6,2,1,""],close:[6,2,1,""],commit:[6,2,1,""],execute:[6,2,1,""],find:[6,2,1,""],flush:[6,2,1,""],get:[6,2,1,""],get_database:[6,2,1,""],invalidate:[6,2,1,""],of:[6,4,1,""],prepare:[6,2,1,""],reload:[6,2,1,""],remove:[6,2,1,""],remove_flush_order:[6,2,1,""],reset:[6,2,1,""],rollback:[6,2,1,""],unblock_access:[6,2,1,""],unblock_implicit_flushes:[6,2,1,""],using:[6,2,1,""]},"storm.tracer":{BaseStatementTracer:[2,1,1,""],DebugTracer:[2,1,1,""],TimeoutTracer:[2,1,1,""],debug:[2,5,1,""],get_tracers:[2,5,1,""],install_tracer:[2,5,1,""],remove_all_tracers:[2,5,1,""],remove_tracer:[2,5,1,""],remove_tracer_type:[2,5,1,""],trace:[2,5,1,""]},"storm.tracer.BaseStatementTracer":{connection_raw_execute:[2,2,1,""]},"storm.tracer.DebugTracer":{connection_commit:[2,2,1,""],connection_raw_execute:[2,2,1,""],connection_raw_execute_error:[2,2,1,""],connection_raw_execute_success:[2,2,1,""],connection_rollback:[2,2,1,""]},"storm.tracer.TimeoutTracer":{connection_commit:[2,2,1,""],connection_raw_execute:[2,2,1,""],connection_raw_execute_error:[2,2,1,""],connection_rollback:[2,2,1,""],get_remaining_time:[2,2,1,""],set_statement_timeout:[2,2,1,""]},"storm.tz":{gettz:[5,5,1,""],tzfile:[5,1,1,""],tzical:[5,1,1,""],tzlocal:[5,1,1,""],tzoffset:[5,1,1,""],tzrange:[5,1,1,""],tzstr:[5,1,1,""],tzutc:[5,1,1,""]},"storm.tz.tzfile":{dst:[5,2,1,""],tzname:[5,2,1,""],utcoffset:[5,2,1,""]},"storm.tz.tzical":{get:[5,2,1,""],keys:[5,2,1,""]},"storm.tz.tzlocal":{dst:[5,2,1,""],tzname:[5,2,1,""],utcoffset:[5,2,1,""]},"storm.tz.tzoffset":{dst:[5,2,1,""],tzname:[5,2,1,""],utcoffset:[5,2,1,""]},"storm.tz.tzrange":{dst:[5,2,1,""],tzname:[5,2,1,""],utcoffset:[5,2,1,""]},"storm.tz.tzutc":{dst:[5,2,1,""],tzname:[5,2,1,""],utcoffset:[5,2,1,""]},"storm.uri":{URI:[5,1,1,""],escape:[5,5,1,""],unescape:[5,5,1,""]},"storm.uri.URI":{copy:[5,2,1,""],database:[5,3,1,""],host:[5,3,1,""],password:[5,3,1,""],port:[5,3,1,""],username:[5,3,1,""]},"storm.variables":{BoolVariable:[0,1,1,""],DateTimeVariable:[0,1,1,""],DateVariable:[0,1,1,""],DecimalVariable:[0,1,1,""],EnumVariable:[0,1,1,""],FloatVariable:[0,1,1,""],IntVariable:[0,1,1,""],JSONVariable:[0,1,1,""],LazyValue:[0,1,1,""],ListVariable:[0,1,1,""],RawStrVariable:[0,1,1,""],TimeDeltaVariable:[0,1,1,""],TimeVariable:[0,1,1,""],UUIDVariable:[0,1,1,""],UnicodeVariable:[0,1,1,""],Variable:[0,1,1,""],VariableFactory:[0,3,1,""]},"storm.variables.BoolVariable":{parse_set:[0,2,1,""]},"storm.variables.DateTimeVariable":{parse_set:[0,2,1,""]},"storm.variables.DateVariable":{parse_set:[0,2,1,""]},"storm.variables.DecimalVariable":{parse_get:[0,4,1,""],parse_set:[0,4,1,""]},"storm.variables.EnumVariable":{parse_get:[0,2,1,""],parse_set:[0,2,1,""]},"storm.variables.FloatVariable":{parse_set:[0,2,1,""]},"storm.variables.IntVariable":{parse_set:[0,2,1,""]},"storm.variables.ListVariable":{get_state:[0,2,1,""],parse_get:[0,2,1,""],parse_set:[0,2,1,""],set_state:[0,2,1,""]},"storm.variables.RawStrVariable":{parse_set:[0,2,1,""]},"storm.variables.TimeDeltaVariable":{parse_set:[0,2,1,""]},"storm.variables.TimeVariable":{parse_set:[0,2,1,""]},"storm.variables.UUIDVariable":{parse_get:[0,2,1,""],parse_set:[0,2,1,""]},"storm.variables.UnicodeVariable":{parse_set:[0,2,1,""]},"storm.variables.Variable":{"delete":[0,2,1,""],checkpoint:[0,2,1,""],column:[0,3,1,""],copy:[0,2,1,""],event:[0,3,1,""],get:[0,2,1,""],get_lazy:[0,2,1,""],get_state:[0,2,1,""],has_changed:[0,2,1,""],is_defined:[0,2,1,""],parse_get:[0,2,1,""],parse_set:[0,2,1,""],set:[0,2,1,""],set_state:[0,2,1,""]},"storm.xid":{Xid:[1,1,1,""]},storm:{base:[5,0,0,"-"],cache:[5,0,0,"-"],database:[1,0,0,"-"],event:[2,0,0,"-"],exceptions:[5,0,0,"-"],expr:[3,0,0,"-"],info:[6,0,0,"-"],properties:[0,0,0,"-"],references:[0,0,0,"-"],store:[6,0,0,"-"],tracer:[2,0,0,"-"],tz:[5,0,0,"-"],uri:[5,0,0,"-"],variables:[0,0,0,"-"],xid:[1,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","attribute","Python attribute"],"4":["py","staticmethod","Python static method"],"5":["py","function","Python function"],"6":["py","exception","Python exception"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:attribute","4":"py:staticmethod","5":"py:function","6":"py:exception"},terms:{"break":6,"byte":1,"case":[0,1,6],"class":[0,1,2,3,5,6],"default":[1,3],"enum":0,"final":6,"float":0,"function":1,"int":[0,3],"long":6,"new":[1,5,6],"return":[1,2,3,5,6],"static":[0,1,6],"throw":6,"true":[0,1,3,5],AND:3,And:3,For:[0,6],NOT:3,Not:3,One:6,The:[1,2,3,5,6],There:3,These:3,Use:5,Used:0,__storm_flushed__:6,__storm_pre_flush__:6,_end:1,_remote_prop:0,_timeout_tracer_remaining_tim:2,abl:0,accept:[0,1],access:[1,6],accord:3,accordingli:0,act:3,actual:0,add:[1,3,5,6],add_class:0,add_flush_ord:6,add_properti:0,added:[3,5,6],affect:1,after:[5,6],again:6,against:3,agnost:1,alia:[0,1,3,6],alias:3,alias_count:6,aliv:6,all:[0,1,3,6],allow:[0,1,2],almost:6,alreadi:[1,5],also:[0,1,3,6],alwai:6,ani:[1,3,5,6],anni:1,anoth:[0,3],anyth:1,api:1,append:1,applic:[2,6],appropri:1,approxim:5,arg1:3,arg2:3,arg:[0,1,2,3,6],argument:[0,3],around:3,asc:3,assert:0,assign:0,associ:[5,6],assumpt:6,attempt:[1,5],attr:3,attr_nam:0,attribut:[0,3,6],auto:3,auto_count:3,auto_t:3,automat:[1,3,6],autoreload:6,autot:3,avail:[3,6],avg:[3,6],avoid:0,awai:6,back:6,backend:[1,2,3,6],backward:0,bar:[0,3],bar_id:0,bar_titl:0,base:[0,1,2,3,5,6],basestatementtrac:2,basic:[1,6],becaus:[1,3],becom:[5,6],been:[1,6],befor:[1,2,5,6],begin:[1,3,6],behav:0,being:[0,2,3,5],between:5,bewar:6,binari:1,binaryexpr:3,binaryop:[1,3],bit:1,blacklist:3,block:[1,5,6],block_access:[1,6],block_implicit_flush:6,bool:[0,3],boolvari:0,both:[0,5],boundari:[5,6],branch_qualifi:1,broadcast:1,buffer:1,build:[3,6],build_raw_cursor:1,build_tabl:3,built:3,bulk:3,bypass:6,cach:[5,6],call:[1,5,6],callabl:1,can:[0,1,3],cancel:6,case_sensit:3,cast:3,caus:[5,6],certain:6,chang:[1,5,6],charact:3,check:[1,2],checkpoint:[0,6],choos:0,classalia:6,classalias:6,classinfo:6,classinfoerror:5,claus:3,clean:0,clear:[0,5],close:[1,6],closederror:5,closer:0,cls:[0,6],cls_info:6,cls_spec:6,coalesc:3,code:[1,6],collect:5,column:[1,3,4,6],column_seq:1,come:5,commit:[1,2,6],common:1,compani:6,company_id:6,compar:[1,3],comparableexpr:[0,3],compare_column:3,compil:[1,3],compile_alia:3,compile_auto_t:3,compile_binary_op:3,compile_bool:3,compile_cach:3,compile_cas:1,compile_cast:3,compile_column:3,compile_compound_op:3,compile_count:3,compile_currv:1,compile_d:3,compile_datetim:3,compile_decim:3,compile_delet:3,compile_distinct:3,compile_eq:3,compile_float:3,compile_func:3,compile_id:3,compile_in:3,compile_insert:3,compile_insert_postgr:1,compile_insert_sqlit:1,compile_int:3,compile_join:3,compile_lik:3,compile_like_postgr:1,compile_list_vari:1,compile_n:3,compile_neg_expr:3,compile_non:3,compile_non_assoc_binary_op:3,compile_prefix_expr:3,compile_python_bool_and_d:3,compile_python_builtin:3,compile_python_column:3,compile_python_sql_token:3,compile_python_unsupport:3,compile_python_vari:3,compile_return:1,compile_select:3,compile_select_sqlit:1,compile_sequence_postgr:1,compile_set_expr:3,compile_set_expr_postgr:1,compile_sql:3,compile_sql_token:3,compile_sql_token_postgr:1,compile_str:3,compile_suffix_expr:3,compile_t:3,compile_tim:3,compile_timedelta:3,compile_unicod:3,compile_upd:3,compile_vari:3,compileerror:5,compilepython:3,compliant:1,compos:6,compoundexpr:3,compoundop:3,condit:[1,2],config:6,configur:6,connect:[1,2,5,6],connection_commit:2,connection_factori:1,connection_raw_execut:2,connection_raw_execute_error:[1,2],connection_raw_execute_success:2,connection_rollback:2,connectionblockederror:[1,5],consid:3,constructor:3,contain:[5,6],contains_str:3,content:4,context:[2,3],conveni:6,convers:1,convert:1,convert_param_mark:1,copi:[0,5,6],copyright:5,count:[3,6],creat:[1,6],create_databas:1,create_from_uri:1,credenti:1,current:[2,3,5,6],currval:1,cursor:[1,2],custom:1,data:[1,3,6],databas:[0,2,3,4,5,6],databaseerror:5,databasemoduleerror:5,dataerror:5,datatyp:1,date:0,datetim:[0,1,5],datetimevari:0,datevari:0,dbapi:1,dealloc:5,debug:[1,2],debugtrac:2,decim:0,decimalvari:0,def:6,default_t:3,defici:3,defin:[0,6],deleg:1,delet:[0,3,6],demot:5,depend:[3,6],desc:3,describ:1,descriptor:0,desir:6,detail:1,detect:[1,6],develop:6,dict:[3,5,6],dictionari:3,differ:[0,6],direct:6,dirti:6,disambigu:0,disappear:6,discard:3,disconnect:1,disconnectionerror:[1,5],distinct:[3,6],div:3,document:4,doe:[1,2,5,6],doesn:1,done:1,drop:5,dsn:1,dst:5,dstabbr:5,dstoffset:5,dure:3,each:6,eas:1,east:5,eat:5,effect:6,either:[0,3,5],element:1,emit:[2,3],employe:6,empti:6,emptyresultset:6,emul:6,encodedvaluevari:0,end:5,endpo:3,endswith:3,enough:2,ensur:6,entir:[5,6],entireti:5,entri:[3,5],enumer:0,enumvari:0,equival:0,error:[0,1,2,5],escap:[1,3,5],etc:[2,6],evalu:0,even:5,event:[0,1,4,6],eventsystem:2,everi:0,evict:5,exact:5,exampl:[0,1,6],exc:1,except:[1,3,5,6],execut:[1,2,6],exist:[3,6],expect:[1,3],explicit:6,explicitli:[3,6],expos:0,expr1:[1,3],expr2:[1,3],expr:[0,1,3,6],exprerror:5,express:[1,4,6],extend:1,extens:5,extern:1,extra_disconnection_error:1,facil:2,factori:[1,3],failur:1,fals:[0,1,3,5,6],featur:6,featureerror:[5,6],fetch:[1,6],few:1,field:1,figur:6,fileobj:5,find:[0,3,6],fire:6,first:[0,5,6],flag:2,floatvari:0,flush:6,follow:[3,6],foo:[0,3,6],foo_alia:6,foreign:0,form:[1,3,6],format:1,format_id:1,found:[0,6],fresh:5,from:[0,1,3,5,6],from_databas:1,from_db:0,from_param_mark:1,fromexpr:3,func:3,funcexpr:[1,3],functool:0,further:1,futur:6,gener:[3,5,6],generationalcach:5,get:[0,1,5,6],get_al:1,get_cach:5,get_cls_info:6,get_databas:6,get_insert_ident:1,get_lazi:0,get_map:0,get_match:3,get_obj:6,get_obj_info:6,get_on:1,get_peopl:6,get_remaining_tim:2,get_select_expr:6,get_stat:0,get_trac:2,get_uri:1,gettz:5,given:[0,1,2,3,6],global:1,global_transaction_id:1,good:6,got:6,granular:[1,2],group_bi:[3,6],guarante:6,gustavo:5,handl:3,handler:1,happen:1,has:[0,1,2,5,6],has_chang:0,has_tabl:3,hasn:1,have:[0,1,3,6],held:5,here:1,high:6,highest:6,hint:3,hold:[5,6],hook:[4,6],host:[1,5],how:[2,3,6],http:2,identif:6,identifi:1,ids:6,immedi:6,implement:[1,2],implicit:6,implicitli:3,increment:3,index:[1,4],indic:5,individu:1,infer:3,info:6,inform:[1,6],inject:3,inner:3,insert:[1,3,6],insid:6,install_except:5,install_trac:2,instanc:[0,1,3,6],instead:[3,5],insuffici:6,integ:3,integrityerror:5,intend:1,interfac:[1,6],interfaceerror:5,intern:3,internalerror:5,interpol:2,intersect:[3,6],intvari:0,invalid:6,involv:1,iow:5,is_defin:0,is_disconnection_error:1,is_empti:6,is_in:[3,6],is_safe_token:3,isn:[2,5],issu:[1,2],item:6,item_factori:0,iter:6,its:[5,6],itself:6,job:6,joe:6,join:[3,6],join_tabl:3,joinexpr:3,json:[0,1],jsonel:1,jsontextel:1,jsonvari:[0,1],just:[1,6],keep:[1,5],kei:[0,1,3,5,6],know:6,kwarg:[0,1,2,3,6],larg:5,last:[3,5,6],later:3,layer:6,lazili:0,lazyvalu:[0,3],leav:5,left:[1,2,3,6],leftjoin:[3,6],level:6,librari:4,like:[0,1,3,6],limit:[3,5,6],link:[0,1],list:[0,1,3,5,6],list_vari:1,listvari:0,live:6,local:[0,1],local_kei:0,local_key1:0,local_key2:0,lock:1,logic:2,look:6,loos:5,lost:1,lostobjecterror:5,lower:3,lru:5,lshift:3,machin:1,made:[5,6],mai:[0,1,3,5,6],make:[1,6],make_dsn:1,manag:6,mani:6,map:[0,3,6],mark:[3,6],marker:[0,3],match:[1,3,6],matter:6,max:[3,6],maximum:5,mayb:3,meant:6,mechan:[1,6],memori:[1,5,6],messag:[1,5],method:[0,1,2,6],min:[3,6],mind:1,minut:5,miscellan:4,mod:3,modif:6,modul:[0,1,4,5,6],more:[1,2,3,5,6],most:[0,5,6],much:[2,5],mul:3,must:[1,2,6],mutablevaluevari:0,my_gui:0,my_guy_id:0,my_sequence_nam:3,mygui:0,name:[0,1,2,3,5,6],namedfunc:3,namespac:0,nativ:0,natur:3,naturaljoin:3,naturalleftjoin:3,naturalrightjoin:3,necessari:[1,3,5,6],need:[3,6],neg:[3,5],neither:3,net:5,never:6,new_valu:3,next:[1,2,3,6],nice:3,niemey:5,nonassocbinaryop:3,none:[0,1,2,3,5,6],noneerror:5,nor:3,noresult:[1,6],normal:[1,6],nostoreerror:5,notableerror:[3,5],note:6,notflushederror:5,notoneerror:5,notsupportederror:5,number:[1,2,5],obj:[0,1,5,6],obj_info:[5,6],object:[0,1,2,3,5,6],objectinfo:6,occur:6,offer:5,offset:[3,5,6],often:[1,6],older:5,on_remot:0,onc:[5,6],one:[0,1,3,6],ones:0,onli:[1,5,6],onto:0,oper:[1,2,3,6],operationalerror:5,option:[1,2,5],order:[5,6],order_bi:[0,3,6],orderlooperror:5,orm:6,other:[1,3,6],other_gui:0,other_guy_id:0,othergui:0,otherwis:[5,6],out:[1,6],outer:3,outsid:1,outstand:6,over:5,overhead:5,overrid:[1,2],overridden:1,overriden:1,overwritten:1,page:4,param:[1,2,3,5,6],param_mark:1,paramet:[1,2,6],paramstyl:1,parenthesi:3,parse_get:0,parse_set:0,part:5,partial:0,particular:6,pass:[1,3,6],password:[1,5],path:0,peculiar:1,pend:[1,6],peopl:6,pep:1,per:6,perform:[1,2,6],permit:1,persist:6,person:6,person_id:6,phase:[1,6],place:0,plu:5,point:6,pop:3,port:5,pos:3,posit:[3,6],possibl:6,postgr:1,postgresconnect:1,postgresql:1,postgresresult:1,postgrestimeouttrac:1,preced:3,prefix:3,prefixexpr:3,prepar:[1,2,6],present:5,preset_primary_kei:1,prevent:[2,5,6],previou:[5,6],previous:[1,2,6],primari:[0,1,3,5,6],primary_column:[1,3],primary_kei:[1,6],primary_key_po:6,primary_var:6,primary_vari:[1,3],print:[1,6],process:[1,3],produc:[3,6],programmingerror:5,prop:0,properti:[0,1,6],propertypatherror:5,propertyregistri:[0,5],provid:[1,2,3,6],proxi:0,psycopg2:1,push:3,pysqlit:1,python:[1,5],queri:[1,2,6],quot:3,rais:[0,1,2,3,5,6],rare:6,rather:0,raw:[1,2],raw_connect:1,raw_cursor:[1,2],raw_execut:1,rawstr:0,rawstrvari:0,reach:5,recent:5,reconnect:1,recov:1,recoveri:1,recreat:6,reduc:5,refer:[0,3,5],referenc:[3,5],referenceset:0,regist:[0,1],register_schem:1,registri:0,relat:6,relationship:0,reload:6,remain:5,remaining_tim:[1,2],rememb:0,remin:5,remote_kei:0,remote_key1:0,remote_key2:0,remote_prop:0,remoteprop:0,remov:[5,6],remove_all_trac:2,remove_flush_ord:6,remove_trac:2,remove_tracer_typ:2,replac:[3,5],repres:[1,3,6],represent:[1,3],request:2,requir:6,reset:[2,6],resolv:0,resort:3,respect:[1,5],restor:3,result:[1,5,6],result_factori:1,resultset:6,retri:1,retriev:[1,6],revert:[3,6],right:[1,3],rightjoin:3,rogu:2,roll:6,rollback:[1,2,6],row:[1,3,6],rowcount:1,rshift:3,run:[1,6],safe:5,same:[3,6],schema:1,scheme:1,search:4,second:[0,1,2],secondari:5,see:[1,5],seen:3,select:[1,3,6],self:[1,6],sequenc:[1,3],server:1,set:[0,1,3,5,6],set_map:0,set_obj:6,set_obj_info:6,set_siz:5,set_stat:0,set_statement_timeout:[1,2],set_vari:1,setexpr:3,setup:[1,2],should:[0,1,2,3,6],shouldn:3,similar:[5,6],simpl:[1,6],simpleproperti:[0,1],sinc:3,singl:[0,1,3],situat:3,size:5,snippet:6,some:[1,3,6],someth:3,somewhat:6,sourc:[0,1,2,3,5,6],special:[1,2],specif:[1,2,3],specifi:[0,1,3,5,6],sql:[1,2,3,6],sqlite:1,sqliteconnect:1,sqliteresult:1,sqlraw:3,sqltoken:3,standard:[1,3,5],start:[5,6],startswith:3,state:[0,1,3,6],statement:[1,2,3,5,6],stdabbr:5,stdoffset:5,still:[5,6],store:[0,3,4,5],storeerror:5,storm:[0,1,2,3,5,6],stormerror:5,str:[1,3],strang:3,stream:2,string:[1,3,5],strong:5,stupidcach:5,sub:3,subclass:[1,2,5],substr:3,subtyp:3,suffix:[1,3],suffixexpr:3,sum:[3,6],suppli:1,support:[1,3],sure:[1,6],system:[1,6],tabl:[0,3,6],table_th:1,table_thecolumn_seq:1,tableset:6,take:[1,2],tell:2,test:[0,1],text:[1,3],than:[0,1,2,3,5],thei:[0,6],them:[1,3,6],theschema:1,thetable_th:1,thetable_thecolumn_seq:1,thi:[0,1,2,3,4,5,6],thing:5,thread:6,threadsaf:6,through:[1,6],thrown:6,time:[0,2,5,6],timedelta:0,timedeltavari:0,timefram:5,timeout:[1,2,5],timeouterror:[1,2,5],timeouttrac:[1,2],timevari:0,titl:0,to_databas:1,to_db:0,to_param_mark:1,token:3,too:[1,5],topmost:3,touch:[0,6],trace:2,tracer:[1,2,5],tracer_typ:2,track:5,transact:[1,2,6],translat:[0,3],tri:[0,3],trigger:6,tupl:[1,3,6],twice:[3,5],two:[0,1,5,6],type:[1,3,4,6],typic:[0,3],tzfile:5,tzical:5,tzid:5,tzinfo:5,tzlocal:5,tzname:5,tzoffset:5,tzrang:5,tzstr:5,tzutc:5,unabl:6,unblock:[1,6],unblock_access:[1,6],unblock_implicit_flush:6,undef:[1,3,6],undefin:1,underli:[1,6],unescap:5,unhook:2,unicod:0,unicodevari:0,uninterest:5,union:[3,6],uniqu:0,unknown:3,unorderederror:5,untouch:1,updat:[0,1,3,6],upper:3,uri:[1,5],uri_str:5,urierror:5,use:[1,2,3,5,6],used:[0,1,2,3,5,6],useful:6,user:[1,5],usernam:5,uses:0,using:[1,6],utc:5,utcoffset:5,uuid:0,uuidvari:0,valu:[0,1,3,6],variabl:[0,1,3,6],variable_class:[0,1],variable_factori:[0,3],variable_kwarg:0,variablefactori:0,variou:3,vendor:5,veri:[3,6],verifi:6,version:1,via:[1,6],wai:3,want:[0,5,6],warn:5,were:0,west:5,when:[0,1,3,5,6],where:[0,1,3,5,6],whether:1,which:[0,1,6],whichev:1,who:6,whose:[0,6],within:[5,6],without:[5,6],won:6,work:[1,3],wrongstoreerror:5,xid:[1,2,6],yet:[1,6],you:[0,5,6],your:[5,6],zero:3,zone:5},titles:["Columns and types","Databases","Hooks and events","Expressions","Storm legacy","Miscellaneous libraries","Store"],titleterms:{column:0,databas:1,event:2,express:3,hook:2,indic:4,legaci:4,librari:5,miscellan:5,store:6,storm:4,tabl:4,type:0}})