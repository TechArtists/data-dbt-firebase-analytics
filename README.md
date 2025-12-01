This library works by default for one Firebase/Google Cloud Project:
# "TA:SOURCES":
    #   - {project_id: google_cloud_project_id,
    #     analytics_dataset_id: schema_id,
    #     events_table: events_table_prefix*,
    #     crashlytics_dataset_id: crashlytics_dataset,
    #     crashlytics_table: crashlytics_table_prefix*}

Adding more project_ids and multiple dataset_ids for specific datasets is also possible.
A few additional steps are required for multiple sources to be added as sources.

1) TA:SOURCES_MULTIPLE_PROJECTS_GENERATED must be set to false (default).
2) add projects and datasets to the TA:SOURCES variable in your dbt project
3) run the following command to generate sources for all projects: 
dbt run-operation -q generate_sources_multiple_projects > models/firebase_sources_multiple_projects.yml 
4) change TA:SOURCES_MULTIPLE_PROJECTS_GENERATED to true
# "TA:SOURCES":
    #   - {project_id: google_cloud_project_id,
    #     analytics_dataset_id: schema_id,
    #     events_table: events_table_prefix*,
    #     crashlytics_dataset_id: crashlytics_dataset,
    #     crashlytics_table: crashlytics_table_prefix*}
    #   - {project_id: google_cloud_project_id2,
    #     analytics_datasets_id: [schema_id,schema_id2],
    #     events_table: events_table_prefix*,
    #     crashlytics_dataset_id: crashlytics_dataset,
    #     crashlytics_table: crashlytics_table_prefix*}



### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


## TODO

- why the DAU counts in app_health (aka _events) doesn't match the ones from raw. There's a dimension in there that's not fully disjunct, maybe make a _events_disjunct table as well
