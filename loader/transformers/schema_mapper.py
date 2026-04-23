from .transform import apply_single_transform

class SchemaMapper:
    def __init__(self, mapping_config):
        self.mapping_config = mapping_config
    
    def transform_value(self, value, transform_config, row=None):
        """
        Apply transform(s) to a value. Suports single of chained transformaations.
        Single trasnforms can be defined in yaml as either a single string or a dict.
        Chained transforms are defined as a list of dicts, each with it's own types and args.
            The order of chained transforms is determined by the order in the list.
        Transforms can output named results using the "as" parameter, which can be referenced in supsequent transforms using $name syntax.
        Args:
            value: The value to be transformed.
            transform_config: The transformation configuration, which can be a single transform or a list of transforms.
            row: The current row being processed, which can be used for field reference in transformations.
        """
        if value is None:
            return None
        
        # Handle single transform (string) - backwards compatible
        if isinstance(transform_config, str):
            return value
        # Handle single transform (dict with type and args)
        if isinstance(transform_config, dict) and "type" in transform_config:
            transform_type = transform_config.get("type")
            args = transform_config.get("args", {})
            return apply_single_transform(value, transform_type, args, row, None)
        
        # Handle chained trasnforms (list of dicts)
        if isinstance(transform_config, list):
            result = value
            named_results = {} # Store named intermediate results

            for transform_step in transform_config:
                transform_type = transform_step.get("type")
                args = transform_step.get("args", {})
                output_name = transform_step.get("as") # Get the "as" parameter for named output

                # Pass named results to the transform
                result = apply_single_transform(result, transform_type, args, row, named_results)

                # Store the result with a name if specified
                if output_name:
                    named_results[output_name] = result
            return result
        return value
    
    def transform(self, data):
        result = []
        for row in data:
            new_row = {}
            for target_field, config in self.mapping_config.items():
                if isinstance(config, (str, int)):
                    # Simple mapping: target - named source ot positional arguement
                    source = config
                    transform_config = None
                    value = row.get(source)
                else:
                    source = config.get('source')
                    position = config.get('position')
                    transform = config.get('transform')
                    args = config.get('args', {})
                    if transform and not isinstance(transform, (list, dict)):
                        transform_config = {'type': transform, 'args': args}
                    else:
                        transform_config = transform
                    
                    if source is not None:
                        value = row.get(source)
                    elif position is not None:
                        value = row.get(position)
                    else:
                        value = None

                if transform_config:
                    value = self.transform_value(value, transform_config, row)
                new_row[target_field] = value
            result.append(new_row)
        return result