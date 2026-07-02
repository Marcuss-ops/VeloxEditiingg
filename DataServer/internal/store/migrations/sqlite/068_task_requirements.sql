-- PR: placement vertical slice — task_requirements table
-- Stores the capability strings a task requires (e.g. "artifact.commit.v1").
-- ListReadyCandidates LEFT JOINs this table to populate
-- placement.TaskCandidate.RequiredCapabilities so the placement
-- matcher can reject tasks whose required capabilities the worker
-- does not advertise.
CREATE TABLE IF NOT EXISTS task_requirements (
    task_id    TEXT NOT NULL,
    capability TEXT NOT NULL,
    PRIMARY KEY (task_id, capability),
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_requirements_task_id
    ON task_requirements(task_id);
