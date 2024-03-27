def run_transform(target_table, write_options):
    def decorator(func):
        def wrapper(*args, **kwargs):
            transformed_df = func(*args, **kwargs)
            write(transformed_df, target_table, write_options)
        return wrapper
    return decorator


def dq_validate(target_table, row_validation_options={}, col_validation_options={}):
    def decorator(func):
        def wrapper(*args,**kwargs):
            transformed_df = func(*args,**kwargs)
            apply_validation(target_table, row_validation_options, col_validation_options)
        return wrapper
    return decorator

