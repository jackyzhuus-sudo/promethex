ALTER TABLE t_user ADD COLUMN earned_invite_points bigint NOT NULL DEFAULT 0;
ALTER TABLE t_user ADD COLUMN provide_invite_points bigint NOT NULL DEFAULT 0;
