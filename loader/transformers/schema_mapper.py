from typing import Any, Dict, List, Optional, Union

import pandas as pd

from .transform import apply_single_transform


class SchemaMapper:
    def __init__(self, mapping_config):
        self.mapping_config = mapping_config

    def transform_value(
        self,
        value: Union[Any, pd.Series],
        transform_config: Union[str, Dict, List],
        row: Optional[Dict[str, Any]] = None,
        df: Optional[pd.DataFrame] = None,
    ) -> Union[Any, pd.Series]:
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
            df: The entire DataFrame being processed, which can be used for transformations that require access to the full dataset.
        Returns:
            The transformed value after applying the specified transformation(s).
        """
        if value is None:
            return None

        is_vectorised = isinstance(value, pd.Series)

        # Handle single transform (string) - backwards compatible
        if isinstance(transform_config, str):
            return value

        # Handle single transform (dict with type and args)
        if isinstance(transform_config, dict) and "type" in transform_config:
            transform_type = str(transform_config.get("type", ""))
            args = dict(transform_config.get("args", {}))
            if is_vectorised:
                return apply_single_transform(
                    value, transform_type, args, row, df, None
                )
            return apply_single_transform(value, transform_type, args, row, None, None)

        # Handle chained trasnforms (list of dicts)
        if isinstance(transform_config, list):
            result = value
            named_results: Dict[str, pd.Series] = {}  # Store named intermediate results

            for transform_step in transform_config:
                transform_type = str(transform_step.get("type", ""))
                args = dict(transform_step.get("args", {}))
                output_name = transform_step.get(
                    "as"
                )  # Get the "as" parameter for named output

                if is_vectorised:
                    result = apply_single_transform(
                        result, transform_type, args, None, df, named_results
                    )
                else:
                    # Pass named results to the transform
                    result = apply_single_transform(
                        result, transform_type, args, row, None, named_results
                    )

                # Store the result with a name if specified
                if output_name and isinstance(result, pd.Series):
                    named_results[str(output_name)] = result
            return result
        return value

    def transform(self, data: Union[pd.DataFrame, List[Dict]]) -> pd.DataFrame:
        if isinstance(data, list):
            input_df = pd.DataFrame(data)
        else:
            input_df = data

        if input_df.empty:
            return input_df

        result_dict: Dict[str, pd.Series] = {}

        for target_field, config in self.mapping_config.items():
            if isinstance(config, (str, int)):
                source = str(config)
                transform_config = None
                value = input_df[source] if source in input_df.columns else None
            else:
                source = config.get("source")
                position = config.get("position")
                transform = config.get("transform")
                args: Dict[str, Any] = dict(config.get("args", {}))
                transform_config = None

                if transform and not isinstance(transform, (list, dict)):
                    transform_config = {"type": transform, "args": args}
                else:
                    transform_config = transform  # type: ignore[assignment]

                if source is not None:
                    value = input_df[source] if source in input_df.columns else None
                elif position is not None:
                    value = (
                        input_df.iloc[:, position]
                        if position < len(input_df.columns)
                        else None
                    )
                else:
                    value = None

            if transform_config:
                value = self.transform_value(value, transform_config, None, input_df)

            if isinstance(value, pd.Series):
                result_dict[str(target_field)] = value
            elif value is not None:
                result_dict[str(target_field)] = pd.Series(
                    [value] * len(input_df), index=input_df.index
                )
            else:
                result_dict[str(target_field)] = pd.Series(
                    [None] * len(input_df), index=input_df.index
                )

        return pd.DataFrame(result_dict, index=input_df.index)
