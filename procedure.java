public enum Direction {
    IN,
    OUT,
    INOUT
}

@Component
@RequiredArgsConstructor
public class GenericProcedureExecutor {
    private final JdbcTemplate jdbcTemplate;
    private final Map<Class<?>, SimpleJdbcCall> cache = new ConcurrentHashMap<>();


    public <T,R> R executeProcedure(T inputDto, Class<R> outputClass) {
        Class<?> inputClass = inputDto.getClass();

        StoredProcedure spAnnotation = inputClass.getAnnotation(StoredProcedure.class);
        if(spAnnotation == null) throw new IllegalArgumentException("Missing @StoredProcedure annotation");

        String procedureName = spAnnotation.name();
        String schema = spAnnotation.schema();

        SimpleJdbcCall jdbcCall = cache.computeIfAbsent(inputClass, cls -> {
            SimpleJdbcCall call = new SimpleJdbcCall(jdbcTemplate)
                    .withoutProcedureColumnMetaDataAccess()
                    .withProcedureName(procedureName);

            if (!schema.isEmpty()) call.withSchemaName(schema);

            List<SqlParameter> parameters = buildSqlParameters(inputClass, outputClass);
            call.declareParameters(parameters.toArray(new SqlParameter[0]));

            // Handle result set
            for (Field field : outputClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(ProcedureResultSet.class)) {
                    ProcedureResultSet rs = field.getAnnotation(ProcedureResultSet.class);
                    call.returningResultSet(rs.name(), new BeanPropertyRowMapper<>(rs.dtoClass()));
                }
            }

            return call;
        });

        Map<String, Object> inParams = extractInputParams(inputDto);
        Map<String, Object> result = jdbcCall.execute(inParams);

        return populateOutput(result, outputClass);
    }

    private Map<String, Object> extractInputParams(Object dto) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : dto.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(ProcedureParam.class)) {
                ProcedureParam param = field.getAnnotation(ProcedureParam.class);
                if (param.direction() == Direction.IN || param.direction() == Direction.INOUT) {
                    field.setAccessible(true);
                    try {
                        Object value = field.get(dto);
                        map.put(param.name().isEmpty() ? field.getName() : param.name(), value);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return map;
    }

    private List<SqlParameter> buildSqlParameters(Class<?> inputClass, Class<?> outputClass) {
        List<SqlParameter> params = new ArrayList<>();

        // Input params
        for (Field field : inputClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(ProcedureParam.class)) {
                ProcedureParam param = field.getAnnotation(ProcedureParam.class);
                String name = param.name().isEmpty() ? field.getName() : param.name();

                SqlParameter sqlParam = null;
                switch (param.direction()) {
                    case IN:
                        sqlParam = new SqlParameter(name, param.sqlType());
                        break;
                    case OUT:
                        sqlParam = new SqlOutParameter(name, param.sqlType());
                        break;
                    case INOUT:
                        sqlParam = new SqlInOutParameter(name, param.sqlType());
                        break;
                }

                if (sqlParam != null) {
                    params.add(sqlParam);
                }
            }
        }

        // Output params
        for (Field field : outputClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(ProcedureParam.class)) {
                ProcedureParam param = field.getAnnotation(ProcedureParam.class);
                String name = param.name().isEmpty() ? field.getName() : param.name();

                SqlParameter sqlParam = null;
                switch (param.direction()) {
                    case OUT:
                        sqlParam = new SqlOutParameter(name, param.sqlType());
                        break;
                    case INOUT:
                        sqlParam = new SqlInOutParameter(name, param.sqlType());
                        break;
                    default:
                        break; // Do nothing for IN
                }

                if (sqlParam != null) {
                    params.add(sqlParam);
                }
            }
        }

        return params;
    }

    private <R> R populateOutput(Map<String, Object> result, Class<R> outputClass) {
        try {
            R dto = outputClass.getDeclaredConstructor().newInstance();

            for (Field field : outputClass.getDeclaredFields()) {
                field.setAccessible(true);

                if (field.isAnnotationPresent(ProcedureParam.class)) {
                    ProcedureParam param = field.getAnnotation(ProcedureParam.class);
                    String paramName = param.name().isEmpty() ? field.getName() : param.name();
                    Object value = result.get(paramName);
                    field.set(dto, value);
                }

                if (field.isAnnotationPresent(ProcedureResultSet.class)) {
                    ProcedureResultSet rs = field.getAnnotation(ProcedureResultSet.class);
                    Object value = result.get(rs.name());
                    field.set(dto, value);
                }
            }

            return dto;

        } catch (Exception e) {
            throw new RuntimeException("Failed to populate output DTO", e);
        }
    }
}
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ProcedureParam {
    Direction direction();
    int sqlType();
    String name() default "";
}


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ProcedureResultSet {
    String name(); // alias in result
    Class<?> dtoClass();
}
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface StoredProcedure {
    String name();
    String schema() default "";
}
