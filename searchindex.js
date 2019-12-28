Search.setIndex({docnames:["api","basic-usage","index","infoheritance"],envversion:{"sphinx.domains.c":1,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":1,"sphinx.domains.javascript":1,"sphinx.domains.math":2,"sphinx.domains.python":1,"sphinx.domains.rst":1,"sphinx.domains.std":1,"sphinx.ext.viewcode":1,sphinx:56},filenames:["api.rst","basic-usage.rst","index.rst","infoheritance.rst"],objects:{"storm.base":{Storm:[0,1,1,""]},"storm.cache":{Cache:[0,1,1,""],GenerationalCache:[0,1,1,""]},"storm.cache.Cache":{add:[0,2,1,""],clear:[0,2,1,""],get_cached:[0,2,1,""],remove:[0,2,1,""],set_size:[0,2,1,""]},"storm.cache.GenerationalCache":{add:[0,2,1,""],clear:[0,2,1,""],get_cached:[0,2,1,""],remove:[0,2,1,""],set_size:[0,2,1,""]},"storm.database":{Connection:[0,1,1,""],Database:[0,1,1,""],Result:[0,1,1,""],convert_param_marks:[0,4,1,""],create_database:[0,4,1,""],register_scheme:[0,4,1,""]},"storm.database.Connection":{begin:[0,2,1,""],block_access:[0,2,1,""],build_raw_cursor:[0,2,1,""],close:[0,2,1,""],commit:[0,2,1,""],compile:[0,3,1,""],execute:[0,2,1,""],is_disconnection_error:[0,2,1,""],param_mark:[0,3,1,""],prepare:[0,2,1,""],preset_primary_key:[0,2,1,""],raw_execute:[0,2,1,""],recover:[0,2,1,""],result_factory:[0,3,1,""],rollback:[0,2,1,""],to_database:[0,2,1,""],unblock_access:[0,2,1,""]},"storm.database.Database":{connect:[0,2,1,""],connection_factory:[0,3,1,""],get_uri:[0,2,1,""],raw_connect:[0,2,1,""]},"storm.database.Result":{close:[0,2,1,""],from_database:[0,2,1,""],get_all:[0,2,1,""],get_insert_identity:[0,2,1,""],get_one:[0,2,1,""],rowcount:[0,2,1,""],set_variable:[0,2,1,""]},"storm.databases":{postgres:[0,0,0,"-"],sqlite:[0,0,0,"-"]},"storm.databases.postgres":{Case:[0,1,1,""],JSON:[0,1,1,""],JSONElement:[0,1,1,""],JSONTextElement:[0,1,1,""],JSONVariable:[0,1,1,""],Postgres:[0,1,1,""],PostgresConnection:[0,1,1,""],PostgresResult:[0,1,1,""],Returning:[0,1,1,""],compile_case:[0,4,1,""],compile_currval:[0,4,1,""],compile_insert_postgres:[0,4,1,""],compile_like_postgres:[0,4,1,""],compile_list_variable:[0,4,1,""],compile_returning:[0,4,1,""],compile_sequence_postgres:[0,4,1,""],compile_set_expr_postgres:[0,4,1,""],compile_sql_token_postgres:[0,4,1,""],create_from_uri:[0,3,1,""],currval:[0,1,1,""],make_dsn:[0,4,1,""]},"storm.databases.postgres.JSON":{variable_class:[0,3,1,""]},"storm.databases.postgres.JSONElement":{oper:[0,3,1,""]},"storm.databases.postgres.JSONTextElement":{oper:[0,3,1,""]},"storm.databases.postgres.Postgres":{connection_factory:[0,3,1,""],raw_connect:[0,2,1,""]},"storm.databases.postgres.PostgresConnection":{compile:[0,3,1,""],execute:[0,2,1,""],is_disconnection_error:[0,2,1,""],param_mark:[0,3,1,""],result_factory:[0,3,1,""],to_database:[0,2,1,""]},"storm.databases.postgres.PostgresResult":{get_insert_identity:[0,2,1,""]},"storm.databases.postgres.currval":{name:[0,3,1,""]},"storm.databases.sqlite":{SQLite:[0,1,1,""],SQLiteConnection:[0,1,1,""],SQLiteResult:[0,1,1,""],compile_insert_sqlite:[0,4,1,""],compile_select_sqlite:[0,4,1,""],create_from_uri:[0,3,1,""]},"storm.databases.sqlite.SQLite":{connection_factory:[0,3,1,""],raw_connect:[0,2,1,""]},"storm.databases.sqlite.SQLiteConnection":{commit:[0,2,1,""],compile:[0,3,1,""],raw_execute:[0,2,1,""],result_factory:[0,3,1,""],rollback:[0,2,1,""],to_database:[0,2,1,""]},"storm.databases.sqlite.SQLiteResult":{from_database:[0,2,1,""],get_insert_identity:[0,2,1,""],set_variable:[0,2,1,""]},"storm.event":{EventSystem:[0,1,1,""]},"storm.event.EventSystem":{emit:[0,2,1,""],hook:[0,2,1,""],unhook:[0,2,1,""]},"storm.exceptions":{ClassInfoError:[0,5,1,""],ClosedError:[0,5,1,""],CompileError:[0,5,1,""],ConnectionBlockedError:[0,5,1,""],DataError:[0,5,1,""],DatabaseError:[0,5,1,""],DatabaseModuleError:[0,5,1,""],DisconnectionError:[0,5,1,""],Error:[0,5,1,""],ExprError:[0,5,1,""],FeatureError:[0,5,1,""],IntegrityError:[0,5,1,""],InterfaceError:[0,5,1,""],InternalError:[0,5,1,""],LostObjectError:[0,5,1,""],NoStoreError:[0,5,1,""],NoTableError:[0,5,1,""],NoneError:[0,5,1,""],NotFlushedError:[0,5,1,""],NotOneError:[0,5,1,""],NotSupportedError:[0,5,1,""],OperationalError:[0,5,1,""],OrderLoopError:[0,5,1,""],ProgrammingError:[0,5,1,""],PropertyPathError:[0,5,1,""],StoreError:[0,5,1,""],StormError:[0,5,1,""],TimeoutError:[0,5,1,""],URIError:[0,5,1,""],UnorderedError:[0,5,1,""],Warning:[0,5,1,""],WrongStoreError:[0,5,1,""],install_exceptions:[0,4,1,""]},"storm.expr":{Add:[0,1,1,""],Alias:[0,1,1,""],And:[0,1,1,""],Asc:[0,1,1,""],AutoTables:[0,1,1,""],Avg:[0,1,1,""],BinaryExpr:[0,1,1,""],BinaryOper:[0,1,1,""],Cast:[0,1,1,""],Coalesce:[0,1,1,""],Column:[0,1,1,""],Comparable:[0,1,1,""],ComparableExpr:[0,1,1,""],CompilePython:[0,1,1,""],CompoundExpr:[0,1,1,""],CompoundOper:[0,1,1,""],Context:[0,1,1,""],Count:[0,1,1,""],Delete:[0,1,1,""],Desc:[0,1,1,""],Distinct:[0,1,1,""],Div:[0,1,1,""],Eq:[0,1,1,""],Except:[0,1,1,""],Exists:[0,1,1,""],Expr:[0,1,1,""],FromExpr:[0,1,1,""],Func:[0,1,1,""],FuncExpr:[0,1,1,""],Ge:[0,1,1,""],Gt:[0,1,1,""],In:[0,1,1,""],Insert:[0,1,1,""],Intersect:[0,1,1,""],Join:[0,1,1,""],JoinExpr:[0,1,1,""],LShift:[0,1,1,""],Le:[0,1,1,""],LeftJoin:[0,1,1,""],Like:[0,1,1,""],Lower:[0,1,1,""],Lt:[0,1,1,""],Max:[0,1,1,""],Min:[0,1,1,""],Mod:[0,1,1,""],Mul:[0,1,1,""],NamedFunc:[0,1,1,""],NaturalJoin:[0,1,1,""],NaturalLeftJoin:[0,1,1,""],NaturalRightJoin:[0,1,1,""],Ne:[0,1,1,""],Neg:[0,1,1,""],NonAssocBinaryOper:[0,1,1,""],Not:[0,1,1,""],Or:[0,1,1,""],PrefixExpr:[0,1,1,""],RShift:[0,1,1,""],RightJoin:[0,1,1,""],Row:[0,1,1,""],SQL:[0,1,1,""],SQLRaw:[0,1,1,""],SQLToken:[0,1,1,""],Select:[0,1,1,""],Sequence:[0,1,1,""],SetExpr:[0,1,1,""],State:[0,1,1,""],Sub:[0,1,1,""],SuffixExpr:[0,1,1,""],Sum:[0,1,1,""],Table:[0,1,1,""],Union:[0,1,1,""],Update:[0,1,1,""],Upper:[0,1,1,""],build_tables:[0,4,1,""],compare_columns:[0,4,1,""],compile_alias:[0,4,1,""],compile_auto_tables:[0,4,1,""],compile_binary_oper:[0,4,1,""],compile_bool:[0,4,1,""],compile_cast:[0,4,1,""],compile_column:[0,4,1,""],compile_compound_oper:[0,4,1,""],compile_count:[0,4,1,""],compile_date:[0,4,1,""],compile_datetime:[0,4,1,""],compile_decimal:[0,4,1,""],compile_delete:[0,4,1,""],compile_distinct:[0,4,1,""],compile_eq:[0,4,1,""],compile_float:[0,4,1,""],compile_func:[0,4,1,""],compile_in:[0,4,1,""],compile_insert:[0,4,1,""],compile_int:[0,4,1,""],compile_join:[0,4,1,""],compile_like:[0,4,1,""],compile_ne:[0,4,1,""],compile_neg_expr:[0,4,1,""],compile_non_assoc_binary_oper:[0,4,1,""],compile_none:[0,4,1,""],compile_prefix_expr:[0,4,1,""],compile_python_bool_and_dates:[0,4,1,""],compile_python_builtin:[0,4,1,""],compile_python_column:[0,4,1,""],compile_python_sql_token:[0,4,1,""],compile_python_unsupported:[0,4,1,""],compile_python_variable:[0,4,1,""],compile_select:[0,4,1,""],compile_set_expr:[0,4,1,""],compile_sql:[0,4,1,""],compile_sql_token:[0,4,1,""],compile_str:[0,4,1,""],compile_suffix_expr:[0,4,1,""],compile_table:[0,4,1,""],compile_time:[0,4,1,""],compile_timedelta:[0,4,1,""],compile_unicode:[0,4,1,""],compile_update:[0,4,1,""],compile_variable:[0,4,1,""],has_tables:[0,4,1,""],is_safe_token:[0,4,1,""]},"storm.expr.Add":{oper:[0,3,1,""]},"storm.expr.Alias":{auto_counter:[0,3,1,""],expr:[0,3,1,""],name:[0,3,1,""]},"storm.expr.And":{oper:[0,3,1,""]},"storm.expr.Asc":{suffix:[0,3,1,""]},"storm.expr.AutoTables":{expr:[0,3,1,""],replace:[0,3,1,""],tables:[0,3,1,""]},"storm.expr.Avg":{name:[0,3,1,""]},"storm.expr.BinaryExpr":{expr1:[0,3,1,""],expr2:[0,3,1,""]},"storm.expr.BinaryOper":{oper:[0,3,1,""]},"storm.expr.Cast":{column:[0,3,1,""],name:[0,3,1,""],type:[0,3,1,""]},"storm.expr.Coalesce":{name:[0,3,1,""]},"storm.expr.Column":{compile_cache:[0,3,1,""],compile_id:[0,3,1,""],name:[0,3,1,""],primary:[0,3,1,""],table:[0,3,1,""],variable_factory:[0,3,1,""]},"storm.expr.Comparable":{contains_string:[0,2,1,""],endswith:[0,2,1,""],is_in:[0,2,1,""],like:[0,2,1,""],lower:[0,2,1,""],startswith:[0,2,1,""],upper:[0,2,1,""]},"storm.expr.CompilePython":{get_matcher:[0,2,1,""]},"storm.expr.CompoundExpr":{exprs:[0,3,1,""]},"storm.expr.CompoundOper":{oper:[0,3,1,""]},"storm.expr.Count":{column:[0,3,1,""],distinct:[0,3,1,""],name:[0,3,1,""]},"storm.expr.Delete":{default_table:[0,3,1,""],table:[0,3,1,""],where:[0,3,1,""]},"storm.expr.Desc":{suffix:[0,3,1,""]},"storm.expr.Distinct":{e:[0,3,1,""],expr:[0,3,1,""],p:[0,3,1,""],r:[0,3,1,""],x:[0,3,1,""]},"storm.expr.Div":{oper:[0,3,1,""]},"storm.expr.Eq":{oper:[0,3,1,""]},"storm.expr.Except":{oper:[0,3,1,""]},"storm.expr.Exists":{prefix:[0,3,1,""]},"storm.expr.Func":{args:[0,3,1,""],name:[0,3,1,""]},"storm.expr.FuncExpr":{name:[0,3,1,""]},"storm.expr.Ge":{oper:[0,3,1,""]},"storm.expr.Gt":{oper:[0,3,1,""]},"storm.expr.In":{oper:[0,3,1,""]},"storm.expr.Insert":{default_table:[0,3,1,""],map:[0,3,1,""],primary_columns:[0,3,1,""],primary_variables:[0,3,1,""],table:[0,3,1,""],values:[0,3,1,""]},"storm.expr.Intersect":{oper:[0,3,1,""]},"storm.expr.Join":{oper:[0,3,1,""]},"storm.expr.JoinExpr":{left:[0,3,1,""],on:[0,3,1,""],oper:[0,3,1,""],right:[0,3,1,""]},"storm.expr.LShift":{oper:[0,3,1,""]},"storm.expr.Le":{oper:[0,3,1,""]},"storm.expr.LeftJoin":{oper:[0,3,1,""]},"storm.expr.Like":{case_sensitive:[0,3,1,""],escape:[0,3,1,""],oper:[0,3,1,""]},"storm.expr.Lower":{name:[0,3,1,""]},"storm.expr.Lt":{oper:[0,3,1,""]},"storm.expr.Max":{name:[0,3,1,""]},"storm.expr.Min":{name:[0,3,1,""]},"storm.expr.Mod":{oper:[0,3,1,""]},"storm.expr.Mul":{oper:[0,3,1,""]},"storm.expr.NamedFunc":{args:[0,3,1,""]},"storm.expr.NaturalJoin":{oper:[0,3,1,""]},"storm.expr.NaturalLeftJoin":{oper:[0,3,1,""]},"storm.expr.NaturalRightJoin":{oper:[0,3,1,""]},"storm.expr.Ne":{oper:[0,3,1,""]},"storm.expr.Neg":{prefix:[0,3,1,""]},"storm.expr.NonAssocBinaryOper":{oper:[0,3,1,""]},"storm.expr.Not":{prefix:[0,3,1,""]},"storm.expr.Or":{oper:[0,3,1,""]},"storm.expr.PrefixExpr":{expr:[0,3,1,""],prefix:[0,3,1,""]},"storm.expr.RShift":{oper:[0,3,1,""]},"storm.expr.RightJoin":{oper:[0,3,1,""]},"storm.expr.Row":{name:[0,3,1,""]},"storm.expr.SQL":{expr:[0,3,1,""],params:[0,3,1,""],tables:[0,3,1,""]},"storm.expr.Select":{columns:[0,3,1,""],default_tables:[0,3,1,""],distinct:[0,3,1,""],group_by:[0,3,1,""],having:[0,3,1,""],limit:[0,3,1,""],offset:[0,3,1,""],order_by:[0,3,1,""],tables:[0,3,1,""],where:[0,3,1,""]},"storm.expr.Sequence":{name:[0,3,1,""]},"storm.expr.SetExpr":{all:[0,3,1,""],exprs:[0,3,1,""],limit:[0,3,1,""],offset:[0,3,1,""],oper:[0,3,1,""],order_by:[0,3,1,""]},"storm.expr.State":{pop:[0,2,1,""],push:[0,2,1,""]},"storm.expr.Sub":{oper:[0,3,1,""]},"storm.expr.SuffixExpr":{expr:[0,3,1,""],suffix:[0,3,1,""]},"storm.expr.Sum":{name:[0,3,1,""]},"storm.expr.Table":{compile_cache:[0,3,1,""],compile_id:[0,3,1,""],name:[0,3,1,""]},"storm.expr.Union":{oper:[0,3,1,""]},"storm.expr.Update":{default_table:[0,3,1,""],map:[0,3,1,""],primary_columns:[0,3,1,""],table:[0,3,1,""],where:[0,3,1,""]},"storm.expr.Upper":{name:[0,3,1,""]},"storm.info":{ClassAlias:[0,1,1,""],ClassInfo:[0,1,1,""],ObjectInfo:[0,1,1,""],get_cls_info:[0,4,1,""],get_obj_info:[0,4,1,""],set_obj_info:[0,4,1,""]},"storm.info.ClassAlias":{alias_count:[0,3,1,""]},"storm.info.ObjectInfo":{checkpoint:[0,2,1,""],cls_info:[0,3,1,""],event:[0,3,1,""],get_obj:[0,2,1,""],primary_vars:[0,3,1,""],set_obj:[0,2,1,""],variables:[0,3,1,""]},"storm.properties":{Bool:[0,1,1,""],Date:[0,1,1,""],DateTime:[0,1,1,""],Decimal:[0,1,1,""],Enum:[0,1,1,""],Float:[0,1,1,""],Int:[0,1,1,""],JSON:[0,1,1,""],List:[0,1,1,""],Property:[0,1,1,""],PropertyRegistry:[0,1,1,""],RawStr:[0,1,1,""],SimpleProperty:[0,1,1,""],Time:[0,1,1,""],TimeDelta:[0,1,1,""],UUID:[0,1,1,""],Unicode:[0,1,1,""]},"storm.properties.Bool":{variable_class:[0,3,1,""]},"storm.properties.Date":{variable_class:[0,3,1,""]},"storm.properties.DateTime":{variable_class:[0,3,1,""]},"storm.properties.Decimal":{variable_class:[0,3,1,""]},"storm.properties.Enum":{variable_class:[0,3,1,""]},"storm.properties.Float":{variable_class:[0,3,1,""]},"storm.properties.Int":{variable_class:[0,3,1,""]},"storm.properties.JSON":{variable_class:[0,3,1,""]},"storm.properties.List":{variable_class:[0,3,1,""]},"storm.properties.PropertyRegistry":{add_class:[0,2,1,""],add_property:[0,2,1,""],clear:[0,2,1,""],get:[0,2,1,""]},"storm.properties.RawStr":{variable_class:[0,3,1,""]},"storm.properties.SimpleProperty":{variable_class:[0,3,1,""]},"storm.properties.Time":{variable_class:[0,3,1,""]},"storm.properties.TimeDelta":{variable_class:[0,3,1,""]},"storm.properties.UUID":{variable_class:[0,3,1,""]},"storm.properties.Unicode":{variable_class:[0,3,1,""]},"storm.references":{Proxy:[0,1,1,""],Reference:[0,1,1,""],ReferenceSet:[0,1,1,""]},"storm.references.Proxy":{RemoteProp:[0,1,1,""],variable_factory:[0,2,1,""]},"storm.store":{EmptyResultSet:[0,1,1,""],Store:[0,1,1,""]},"storm.store.EmptyResultSet":{any:[0,2,1,""],avg:[0,2,1,""],cached:[0,2,1,""],config:[0,2,1,""],copy:[0,2,1,""],count:[0,2,1,""],difference:[0,2,1,""],find:[0,2,1,""],first:[0,2,1,""],get_select_expr:[0,2,1,""],group_by:[0,2,1,""],intersection:[0,2,1,""],is_empty:[0,2,1,""],last:[0,2,1,""],max:[0,2,1,""],min:[0,2,1,""],one:[0,2,1,""],order_by:[0,2,1,""],remove:[0,2,1,""],set:[0,2,1,""],sum:[0,2,1,""],union:[0,2,1,""],values:[0,2,1,""]},"storm.store.Store":{add:[0,2,1,""],add_flush_order:[0,2,1,""],autoreload:[0,2,1,""],begin:[0,2,1,""],block_access:[0,2,1,""],block_implicit_flushes:[0,2,1,""],close:[0,2,1,""],commit:[0,2,1,""],execute:[0,2,1,""],find:[0,2,1,""],flush:[0,2,1,""],get:[0,2,1,""],get_database:[0,2,1,""],invalidate:[0,2,1,""],of:[0,2,1,""],prepare:[0,2,1,""],reload:[0,2,1,""],remove:[0,2,1,""],remove_flush_order:[0,2,1,""],reset:[0,2,1,""],rollback:[0,2,1,""],unblock_access:[0,2,1,""],unblock_implicit_flushes:[0,2,1,""],using:[0,2,1,""]},"storm.tracer":{BaseStatementTracer:[0,1,1,""],DebugTracer:[0,1,1,""],debug:[0,4,1,""],get_tracers:[0,4,1,""],install_tracer:[0,4,1,""],remove_all_tracers:[0,4,1,""],remove_tracer:[0,4,1,""],remove_tracer_type:[0,4,1,""],trace:[0,4,1,""]},"storm.tracer.BaseStatementTracer":{connection_raw_execute:[0,2,1,""]},"storm.tracer.DebugTracer":{connection_commit:[0,2,1,""],connection_raw_execute:[0,2,1,""],connection_raw_execute_error:[0,2,1,""],connection_raw_execute_success:[0,2,1,""],connection_rollback:[0,2,1,""]},"storm.tz":{gettz:[0,4,1,""],tzfile:[0,1,1,""],tzical:[0,1,1,""],tzlocal:[0,1,1,""],tzoffset:[0,1,1,""],tzrange:[0,1,1,""],tzstr:[0,1,1,""],tzutc:[0,1,1,""]},"storm.tz.tzfile":{dst:[0,2,1,""],tzname:[0,2,1,""],utcoffset:[0,2,1,""]},"storm.tz.tzical":{get:[0,2,1,""],keys:[0,2,1,""]},"storm.tz.tzlocal":{dst:[0,2,1,""],tzname:[0,2,1,""],utcoffset:[0,2,1,""]},"storm.tz.tzoffset":{dst:[0,2,1,""],tzname:[0,2,1,""],utcoffset:[0,2,1,""]},"storm.tz.tzrange":{dst:[0,2,1,""],tzname:[0,2,1,""],utcoffset:[0,2,1,""]},"storm.tz.tzutc":{dst:[0,2,1,""],tzname:[0,2,1,""],utcoffset:[0,2,1,""]},"storm.uri":{URI:[0,1,1,""],escape:[0,4,1,""],unescape:[0,4,1,""]},"storm.uri.URI":{copy:[0,2,1,""],database:[0,3,1,""],host:[0,3,1,""],password:[0,3,1,""],port:[0,3,1,""],username:[0,3,1,""]},"storm.variables":{BoolVariable:[0,1,1,""],DateTimeVariable:[0,1,1,""],DateVariable:[0,1,1,""],DecimalVariable:[0,1,1,""],EnumVariable:[0,1,1,""],FloatVariable:[0,1,1,""],IntVariable:[0,1,1,""],JSONVariable:[0,1,1,""],LazyValue:[0,1,1,""],ListVariable:[0,1,1,""],RawStrVariable:[0,1,1,""],TimeDeltaVariable:[0,1,1,""],TimeVariable:[0,1,1,""],UUIDVariable:[0,1,1,""],UnicodeVariable:[0,1,1,""],Variable:[0,1,1,""],VariableFactory:[0,3,1,""]},"storm.variables.BoolVariable":{parse_set:[0,2,1,""]},"storm.variables.DateTimeVariable":{parse_set:[0,2,1,""]},"storm.variables.DateVariable":{parse_set:[0,2,1,""]},"storm.variables.DecimalVariable":{parse_get:[0,2,1,""],parse_set:[0,2,1,""]},"storm.variables.EnumVariable":{parse_get:[0,2,1,""],parse_set:[0,2,1,""]},"storm.variables.FloatVariable":{parse_set:[0,2,1,""]},"storm.variables.IntVariable":{parse_set:[0,2,1,""]},"storm.variables.ListVariable":{get_state:[0,2,1,""],parse_get:[0,2,1,""],parse_set:[0,2,1,""],set_state:[0,2,1,""]},"storm.variables.RawStrVariable":{parse_set:[0,2,1,""]},"storm.variables.TimeDeltaVariable":{parse_set:[0,2,1,""]},"storm.variables.TimeVariable":{parse_set:[0,2,1,""]},"storm.variables.UUIDVariable":{parse_get:[0,2,1,""],parse_set:[0,2,1,""]},"storm.variables.UnicodeVariable":{parse_set:[0,2,1,""]},"storm.variables.Variable":{"delete":[0,2,1,""],checkpoint:[0,2,1,""],column:[0,3,1,""],copy:[0,2,1,""],event:[0,3,1,""],get:[0,2,1,""],get_lazy:[0,2,1,""],get_state:[0,2,1,""],has_changed:[0,2,1,""],is_defined:[0,2,1,""],parse_get:[0,2,1,""],parse_set:[0,2,1,""],set:[0,2,1,""],set_state:[0,2,1,""]},"storm.xid":{Xid:[0,1,1,""]},storm:{base:[0,0,0,"-"],cache:[0,0,0,"-"],database:[0,0,0,"-"],event:[0,0,0,"-"],exceptions:[0,0,0,"-"],expr:[0,0,0,"-"],info:[0,0,0,"-"],properties:[0,0,0,"-"],references:[0,0,0,"-"],store:[0,0,0,"-"],tracer:[0,0,0,"-"],tz:[0,0,0,"-"],uri:[0,0,0,"-"],variables:[0,0,0,"-"],xid:[0,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","attribute","Python attribute"],"4":["py","function","Python function"],"5":["py","exception","Python exception"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:attribute","4":"py:function","5":"py:exception"},terms:{"break":0,"byte":0,"case":[0,1],"class":[0,2],"default":[0,1],"enum":0,"export":[0,1],"final":0,"float":0,"function":[0,1,3],"import":[1,3],"int":[0,1,3],"long":[0,1],"new":[0,1],"null":3,"return":[0,1,3],"static":0,"super":3,"throw":0,"true":[0,1,3],"try":1,"while":1,AND:[0,1],And:[0,3],For:[0,1],Mrs:3,NOT:[0,3],Not:[0,1],One:[0,1],Such:1,That:1,The:[0,2],Then:1,There:[0,1],These:0,Use:0,Used:0,Using:1,Yes:1,__init__:[1,3],__new__:3,__storm_flushed__:0,__storm_loaded__:1,__storm_pre_flush__:0,__storm_primary__:1,__storm_table__:[1,3],_end:0,_info:3,_remote_prop:0,abl:[0,3],abort:1,about:1,abov:[1,3],accept:0,access:[0,1],accord:0,accordingli:0,account:1,accountant_id:1,achiev:1,act:[0,1],actual:[0,1],add:[0,1,3],add_class:0,add_flush_ord:0,add_properti:0,added:[0,1],adding:1,addit:3,advantag:1,affect:[0,1],after:[0,1],again:[0,1,3],against:0,agnost:[0,1],alia:0,alias:[0,2],alias_count:0,aliv:[0,1],all:[0,1,3],allow:[0,1,3],allow_non:3,almost:0,alon:1,alreadi:[0,1],also:[0,1,3],alwai:[0,1],among:1,ani:[0,1],anni:0,anoth:[0,1],anotheremploye:1,anyon:1,anyth:0,api:[1,2],append:0,applic:0,appropri:0,approxim:0,aren:1,arg1:0,arg2:0,arg:0,argument:0,around:[0,3],asc:0,ask:1,assert:[0,1,3],assign:[0,1],associ:[0,1],assumpt:0,attempt:0,attend:1,attent:1,attr:0,attr_nam:0,attribut:[0,1,3],auto:[0,2],auto_count:0,auto_t:0,automat:[0,1],autoreload:[0,1],autot:0,avail:[0,1,3],avg:0,avoid:[0,1],awai:0,back:[0,1],backend:0,backward:0,bar:0,bar_id:0,bar_titl:0,base:[2,3],basestatementtrac:0,basic:[0,2,3],becaus:[0,1,3],becom:0,been:[0,1],befor:[0,1,3],begin:0,behav:0,behavior:2,behaviour:3,behind:1,being:[0,1,3],below:1,ben:1,benefit:[1,3],besid:1,better:1,between:0,bewar:0,bill:1,binari:0,binaryexpr:0,binaryop:0,bit:0,blacklist:0,block:0,block_access:0,block_implicit_flush:0,bodi:1,bool:0,boolvari:0,both:[0,1],bother:1,bound:1,boundari:0,branch_qualifi:0,brazil:1,brl:1,broadcast:0,buffer:0,build:0,build_raw_cursor:0,build_tabl:0,built:[0,1],bulk:0,bypass:0,cach:2,call:[0,1],callabl:0,cameron:3,can:[0,1,3],cancel:0,care:1,case_sensit:0,casper:3,cast:0,caus:0,certain:[0,1,3],certainli:1,chang:[0,2],charact:0,check:[0,1],checkpoint:0,choos:0,circu:1,circular:1,classalia:[0,1],classalias:0,classinfo:0,classinfoerror:0,claus:0,clean:0,clear:0,close:0,closederror:0,closer:0,cls:0,cls_info:0,cls_spec:0,coalesc:0,code:[0,1,3],cohen:3,collect:1,column:[2,3],column_seq:0,come:[0,3],commit:[0,1,3],common:0,compani:[0,1],company_account:1,company_id:[0,1],companyaccount:1,compar:0,comparableexpr:0,compare_column:0,compil:0,compile_alia:0,compile_auto_t:0,compile_binary_op:0,compile_bool:0,compile_cach:0,compile_cas:0,compile_cast:0,compile_column:0,compile_compound_op:0,compile_count:0,compile_currv:0,compile_d:0,compile_datetim:0,compile_decim:0,compile_delet:0,compile_distinct:0,compile_eq:0,compile_float:0,compile_func:0,compile_id:0,compile_in:0,compile_insert:0,compile_insert_postgr:0,compile_insert_sqlit:0,compile_int:0,compile_join:0,compile_lik:0,compile_like_postgr:0,compile_list_vari:0,compile_n:0,compile_neg_expr:0,compile_non:0,compile_non_assoc_binary_op:0,compile_prefix_expr:0,compile_python_bool_and_d:0,compile_python_builtin:0,compile_python_column:0,compile_python_sql_token:0,compile_python_unsupport:0,compile_python_vari:0,compile_return:0,compile_select:0,compile_select_sqlit:0,compile_sequence_postgr:0,compile_set_expr:0,compile_set_expr_postgr:0,compile_sql:0,compile_sql_token:0,compile_sql_token_postgr:0,compile_str:0,compile_suffix_expr:0,compile_t:0,compile_tim:0,compile_timedelta:0,compile_unicod:0,compile_upd:0,compile_vari:0,compileerror:0,compilepython:0,complet:0,complex:1,compliant:0,compos:[0,2],composit:[1,3],compoundexpr:0,compoundop:0,condit:0,config:0,configur:0,connect:[0,1],connection_commit:0,connection_factori:0,connection_raw_execut:0,connection_raw_execute_error:0,connection_raw_execute_success:0,connection_rollback:0,connectionblockederror:0,consid:[0,1,3],constructor:[0,2],contain:0,contains_str:0,content:2,context:0,continu:3,control:1,conveni:[0,1],convers:0,convert:[0,3],convert_param_mark:0,copi:0,copyright:0,could:1,count:[0,1],countri:1,coupl:1,cours:1,creat:[0,2],create_databas:[0,1,3],create_from_uri:0,credenti:0,curiou:1,currenc:1,currency_id:1,current:0,currval:0,cursor:0,custom:[0,1],data:[0,1,3],databas:[2,3],databaseerror:0,databasemoduleerror:0,databs:1,dataerror:0,datatyp:0,date:0,datetim:0,datetimevari:0,datevari:0,dbapi:0,dbname:1,dealloc:0,debug:[0,2],debugtrac:0,decim:0,decimalvari:0,declar:1,decor:0,def:[0,1,3],default_t:0,defici:0,defin:[0,1,2],definit:[2,3],definiton:1,del:[1,3],deleg:0,delet:0,demonstr:1,demot:0,depend:0,desc:[0,1],descend:1,describ:[0,1,3],descriptor:0,design:[1,3],desir:0,detail:0,detect:0,develop:0,dick:3,dict:0,dictionari:0,didn:1,differ:[0,1,3],difficult:3,direct:0,directli:3,dirti:[0,1],disabl:1,disadvantag:1,disambigu:0,disappear:0,discard:0,disconnect:0,disconnectionerror:0,discov:3,distinct:[0,1],div:0,document:2,doe:0,doesn:[0,1,3],doing:1,don:1,done:[0,1],drop:0,dsn:0,dst:0,dstabbr:0,dstoffset:0,due:1,dure:0,each:[0,1,3],earl:1,eas:0,easi:[1,3],easier:[1,3],easili:[1,3],east:0,easton:1,eat:0,effect:0,either:[0,1],element:0,elementari:3,els:3,elsewher:1,emit:0,employe:[0,1,3],empti:0,emptyresultset:0,emul:0,enabl:1,encodedvaluevari:0,end:0,endswith:0,ensur:[0,1,3],entir:[0,1],entireti:0,entri:0,enumer:0,enumvari:0,equival:0,erm:1,error:0,escap:0,etc:0,evalu:0,even:[0,1],event:[1,2],eventsystem:0,ever:1,everi:[0,1],everyth:1,evict:0,exact:0,exampl:[0,1,3],exc:0,execut:[0,2,3],exercis:1,exist:[0,1],existing_info_class:3,expect:[0,1],explicit:[0,1],explicitli:[0,1],expos:[0,1],expr1:0,expr2:0,expr:0,exprerror:0,express:2,extend:0,extens:0,extern:0,extra:1,extra_disconnection_error:0,fact:1,factori:0,failur:0,fals:[0,1,3],far:1,featur:[0,1],featureerror:0,fetch:[0,1],few:[0,1],field:[0,1],figur:0,fileobj:0,find:[0,2,3],fire:0,first:[0,1],flag:0,floatvari:0,flush:[0,2],follow:[0,1,3],foo:0,foo_alia:0,forc:1,foreign:[0,1],form:0,format:[0,1],format_id:0,found:0,fourt:1,frank:1,frequent:1,fresh:0,friendli:3,from:[0,1,3],from_databas:0,from_db:0,from_param_mark:0,fromexpr:0,fun:1,func:0,funcexpr:0,functool:0,further:[0,1],futur:0,garbag:1,garri:1,gener:[0,1],generationalcach:0,get:[0,1,3],get_al:0,get_cach:0,get_cls_info:0,get_databas:0,get_insert_ident:0,get_lazi:0,get_map:0,get_match:0,get_obj:0,get_obj_info:0,get_on:[0,1],get_peopl:0,get_person:3,get_select_expr:0,get_stat:0,get_trac:0,get_uri:0,gettz:0,ghost:3,give:1,given:[0,1,3],glare:1,global:0,global_transaction_id:0,going:[1,3],good:[0,1],got:[0,1],great:3,greater:1,group_bi:0,guarante:0,gustavo:0,had:1,hah:1,handi:3,handl:0,handler:0,happen:[0,1],has:[0,1,3],has_chang:0,has_tabl:0,hasattr:3,hasn:[0,1],have:[0,1,3],haven:1,hei:1,held:0,help:1,here:[0,1,3],hierarchi:3,high:0,higher:1,highest:0,him:1,hint:0,his:1,hold:[0,1],hood:1,hook:2,host:0,hostnam:1,how:[0,1],idea:1,identif:0,identifi:0,ids:[0,1],imagin:1,immedi:0,implement:[0,1,3],implicit:[0,1],implicitli:[0,1],inc:1,includ:1,inconveni:1,increment:0,index:[0,2],indic:0,individu:0,infer:0,info:[1,2],info_class:3,info_typ:3,infoherit:2,inform:[0,1],inherit:[1,3],inject:0,inner:0,insert:[0,1],insid:[0,1],install_except:0,install_trac:0,instanc:[0,1,3],instead:[0,1,3],insuffici:0,integ:[0,1,3],integrityerror:0,intend:0,interest:1,interfac:0,interfaceerror:0,intern:[0,1],internalerror:0,interpol:0,intersect:0,introduc:1,intvari:0,invalid:[0,1],involv:[0,1],iow:0,is_defin:0,is_disconnection_error:0,is_empti:0,is_in:[0,1,3],is_safe_token:0,isn:[0,1,3],issu:1,item:0,item_factori:0,iter:0,its:[0,1],itself:[0,1],job:0,joe:[0,1],john:1,join:[0,2],join_tabl:0,joinexpr:0,json:0,jsonel:0,jsontextel:0,jsonvari:0,just:[0,1],karl:1,keep:[0,1,3],kei:[0,2,3],kent:1,kind:[1,3],know:[0,1],known:1,kwarg:[0,3],larg:0,last:0,later:[0,1],laura:1,layer:0,lazi:1,lazili:0,lazyvalu:0,lead:1,least:1,leav:[0,1],left:0,leftjoin:0,let:[1,3],level:[0,1],life:1,like:[0,1],limit:[0,2,3],line:1,link:[0,1],list:[0,1],list_vari:0,listvari:0,live:0,load:[2,3],local:[1,2,3],local_kei:0,local_key1:0,local_key2:0,lock:0,log:1,logic:1,longer:1,look:[0,3],loop:1,loos:0,lost:0,lostobjecterror:0,lower:0,lru:0,lshift:0,machin:0,made:0,maggi:1,magic:1,mai:[0,1,3],make:[0,1,3],make_dsn:0,manag:0,mani:[0,2],manual:1,map:[0,1],margaret:1,mari:1,mark:0,marker:0,match:[0,1],matter:0,max:0,maximum:0,mayb:0,mayer:1,mean:1,meant:0,mechan:[0,1],memori:[0,1,2],mental:1,messag:0,method:[0,1],migrat:3,mike:1,min:0,mind:0,miscellan:2,miss:1,mod:0,model:[1,2],modif:0,modul:[0,1,2],moment:1,montgomeri:1,more:[0,1,3],most:[0,1],much:0,mul:0,multi:3,multipl:[2,3],music:1,must:[0,1,3],mutablevaluevari:0,my_gui:0,my_guy_id:0,my_sequence_nam:0,mygui:0,name:[0,1,3],namedfunc:0,namespac:[0,1],nativ:0,natur:[0,3],naturaljoin:0,naturalleftjoin:0,naturalrightjoin:0,necessari:[0,1,3],need:[0,1,3],neg:0,neither:0,net:0,never:0,new_valu:0,next:[0,1],nice:1,niemey:0,nonassocbinaryop:0,none:[0,1,3],noneerror:0,nor:0,noresult:[0,1],normal:[0,1,3],nostoreerror:0,notableerror:0,note:[0,1,3],notflushederror:0,noth:1,notic:1,notifi:1,notoneerror:0,notsupportederror:0,now:[1,3],number:[0,1],obj:0,obj_info:0,object:[0,2],objectinfo:0,obtain:1,obviou:1,occur:[0,1],off:1,offer:[0,1],offset:0,often:[0,1],older:0,on_remot:0,onc:[0,1],one:[0,2,3],ones:[0,3],onli:[0,1],onto:0,oper:[0,1],operationalerror:0,option:[0,1],order:[0,2],order_bi:[0,1,3],orderlooperror:0,origin:1,orm:0,other:[0,1],other_gui:0,other_guy_id:0,othergui:0,otherwis:0,our:[1,3],out:[0,1],outer:0,outsid:0,outstand:0,over:0,overhead:0,overrid:0,overridden:0,overriden:0,overwritten:0,page:2,pai:1,pair:1,param:0,param_mark:0,paramet:[0,1],paramstyl:0,parent:3,parenthesi:0,parse_get:0,parse_set:0,part:[0,1],partial:0,particular:0,pass:[0,1],passcod:3,password:[0,1],path:0,pattern:2,peculiar:0,pend:[0,1],peopl:[0,1],pep:0,per:0,perform:0,permit:0,persist:[0,2],person:[0,1,3],person_id:[0,3],person_info_typ:3,personinfo:3,personwithhook:1,phase:0,place:[0,3],plai:1,pleas:1,plu:0,point:[0,1],pop:0,port:[0,1],posit:0,possibl:[0,1],postgr:[0,1],postgresconnect:0,postgresql:1,postgresresult:0,potenti:1,preced:0,prefix:0,prefixexpr:0,prepar:0,present:[0,1],preset_primary_kei:0,pretti:1,prevent:[0,1],previou:0,previous:0,primari:[0,1,3],primary_column:0,primary_kei:0,primary_key_po:0,primary_var:0,primary_vari:0,print:[0,1],problem:3,process:0,produc:0,programmingerror:0,prop:0,properti:[1,3],propertypatherror:0,propertyregistri:0,provid:[0,1,3],proxi:0,psycopg2:0,push:0,pysqlit:0,python:0,queri:[0,2],quiz:1,quot:0,rais:[0,3],rare:0,rather:[0,1,3],raw:0,raw_connect:0,raw_cursor:0,raw_execut:0,rawstr:0,rawstrvari:0,reach:0,reader:1,real:1,realli:1,recent:[0,1],reconnect:0,recov:0,recoveri:0,recreat:0,redo:1,reduc:0,refer:[2,3],referenc:[0,1],referenceset:[0,1],reflect:1,regist:[0,1,2],register_person_info_typ:3,register_schem:0,registri:0,relat:[0,1],relationship:[0,1],reload:[0,2],remain:0,rememb:0,remin:0,remote_kei:0,remote_key1:0,remote_key2:0,remote_prop:0,remoteprop:0,remov:[0,1],remove_all_trac:0,remove_flush_ord:0,remove_trac:0,remove_tracer_typ:0,replac:0,repres:[0,1,3],represent:0,requir:[0,1,3],reset:0,resolv:[0,1],resort:0,respect:0,restor:0,result:[0,2,3],result_factori:0,resultset:0,retri:0,retriev:[0,1,2],revers:1,revert:0,right:[0,1],rightjoin:0,ritcher:1,roll:[0,1],rollback:[0,1,3],row:0,rowcount:0,rshift:0,rui:1,run:[0,3],runtimeerror:3,safe:0,sai:1,same:[0,1,3],sampl:2,scene:1,schema:0,scheme:[0,1],school:3,scope:1,search:2,second:0,secondari:0,secret:3,secret_ag:3,secretag:3,see:[0,1],seen:[0,1],select:[0,2],self:[0,1,3],sequenc:0,server:0,set:[0,2],set_map:0,set_obj:0,set_obj_info:0,set_siz:0,set_stat:0,set_vari:0,setexpr:0,setup:0,sever:1,should:[0,1],shouldn:0,show:[0,1],shown:1,similar:0,simpl:[0,1],simpleproperti:0,simplest:1,simpli:3,sinc:[0,1],singl:[0,1,3],situat:[0,1],size:0,snippet:0,socket:1,some:[0,1],someth:[0,1],sometim:1,somewhat:0,somewher:1,soon:1,sort:1,sound:1,sourc:0,special:1,specif:[0,3],specifi:0,sql:[0,1],sqlite:[1,3],sqliteconnect:0,sqliteresult:0,sqlraw:0,sqltoken:0,stabl:3,stai:1,standard:0,start:[0,1],startswith:0,state:0,statement:[0,1],stdabbr:0,stderr:1,stdoffset:0,stdout:1,step:1,stick:1,still:[0,1],store:[2,3],storedpersoninfo:3,storeerror:0,storm:[0,3],storm_tabl:1,stormerror:0,str:0,straightforward:1,strang:0,stream:[0,1],strictli:3,string:0,stringifi:1,strong:0,stupidcach:0,sub:[0,2],subclass:[0,2,3],subselect:1,substr:0,subtyp:0,suffix:0,suffixexpr:0,sum:0,suppli:0,support:[0,1,3],suppos:1,sure:[0,3],suspens:1,sweet:1,symbol:1,sys:1,system:[0,1],tabl:[0,1,3],table_th:0,table_thecolumn_seq:0,tableset:0,take:0,talk:1,teacher:3,temporari:1,test:0,text:[0,3],than:[0,1,3],thei:[0,1],them:[0,1,3],theschema:0,thetable_th:0,thetable_thecolumn_seq:0,thi:[0,1,2,3],thing:[1,3],thoma:1,though:1,thousand:1,thread:0,threadsaf:0,three:3,through:0,thrown:0,thu:1,tied:1,time:[0,1],timedelta:0,timedeltavari:0,timefram:0,timeout:0,timeouterror:0,timevari:0,titl:0,to_databas:0,to_db:0,to_param_mark:0,token:0,tom:1,too:[0,1],tool:1,top:1,topmost:0,touch:[0,1],trace:[0,1],tracer:1,tracer_typ:0,traci:3,track:0,transact:[0,1],translat:0,tri:[0,1],trigger:[0,1],tupl:[0,1],turn:1,twice:0,two:[0,1],type:[2,3],typic:0,tzfile:0,tzical:0,tzid:0,tzinfo:0,tzlocal:0,tzname:0,tzoffset:0,tzrang:0,tzstr:0,tzutc:0,unabl:0,unblock:0,unblock_access:0,unblock_implicit_flush:0,uncommit:1,undef:0,undefin:[0,1],under:1,underli:0,understand:1,undo:1,unescap:0,unflush:1,unhook:0,unicod:[0,1,3],unicodevari:0,union:0,uniqu:[0,3],unix:1,unknown:0,unorderederror:0,untouch:0,updat:[0,1],upper:0,uri:1,uri_str:0,urierror:0,usag:2,use:[0,1,3],used:[0,1,3],useful:[0,1],user:[0,1],usernam:0,uses:[0,3],using:[0,1,3],usual:1,utc:0,utcoffset:0,uuid:0,uuidvari:0,valu:[0,2,3],varchar:1,variable_class:0,variable_factori:0,variable_kwarg:0,variablefactori:0,variou:0,veri:[0,1,3],verifi:[0,1],version:[0,1],via:0,wai:[0,1,3],want:[0,1,3],warn:0,wasn:1,well:1,were:[0,1],west:0,what:1,whatev:1,when:[0,1,3],where:[0,1,3],whether:0,which:[0,1],whichev:0,who:[0,1],whose:0,why:1,within:[0,1],without:[0,1,3],woah:1,won:[0,1],word:1,work:[0,1,3],worth:1,would:[1,3],write:1,wrongstoreerror:0,yeah:1,yet:[0,1],yield:3,you:[0,1,3],your:0,zero:0,zone:0},titles:["API","Basic usage","Storm legacy","Infoheritance"],titleterms:{"class":[1,3],The:[1,3],alias:1,api:0,auto:1,base:[0,1],basic:1,behavior:1,cach:[0,1],chang:1,column:0,compos:1,constructor:1,creat:[1,3],databas:[0,1],debug:1,defin:3,definit:1,event:0,except:0,execut:1,express:[0,1],find:1,flush:1,hook:[0,1],indic:2,info:[0,3],infoherit:3,join:1,kei:1,legaci:2,limit:1,load:1,local:0,mani:1,memori:3,miscellan:0,model:3,multipl:1,object:[1,3],one:1,order:1,pattern:3,persist:1,postgresql:0,properti:0,queri:1,refer:[0,1],regist:3,reload:1,result:1,retriev:3,sampl:3,select:1,set:1,sqlite:0,store:[0,1],storm:[1,2],sub:1,subclass:1,tabl:2,timezon:0,tracer:0,type:[0,1],uri:0,usag:1,valu:1,variabl:0,xid:0}})