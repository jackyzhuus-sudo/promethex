package bayes_http

import (
	"context"
	"market-backend/internal/pkg/util"

	bayespb "market-proto/proto/market-backend/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"
)

func (s *BayesHttpService) GetTasks(ctx context.Context, req *bayespb.GetTasksRequest) (*bayespb.GetTasksReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	GetTasksRsp, err := s.data.RpcClient.UsercenterClient.GetTasks(ctx, &usercenterpb.GetTasksRequest{
		Uid:    uid,
		IsShow: 1,
	})
	if err != nil {
		c.Log.Errorf("GetTasks rpc failed: %v", err)
		return nil, err
	}
	if len(GetTasksRsp.Tasks) == 0 {
		return &bayespb.GetTasksReply{}, nil
	}

	rsp := &bayespb.GetTasksReply{
		TotalRewardPoints: GetTasksRsp.TotalRewardPoints,
		Total:             GetTasksRsp.Total,
		Tasks:             []*bayespb.GetTasksReply_Task{},
	}
	for _, task := range GetTasksRsp.Tasks {
		rsp.Tasks = append(rsp.Tasks, &bayespb.GetTasksReply_Task{
			TaskUuid:     task.TaskUuid,
			TaskKey:      task.TaskKey,
			Name:         task.Name,
			Desc:         task.Desc,
			Type:         task.Type,
			PicUrl:       task.PicUrl,
			RewardPoints: task.RewardPoints,
			JumpUrl:      task.JumpUrl,
			IsDone:       task.IsDone,
			IsClaimed:    task.IsClaimed,
			ClaimAt:      task.ClaimAt,
			TxHash:       task.TxHash,
			TxStatus:     task.TxStatus,
		})
	}

	return rsp, nil
}

func (s *BayesHttpService) ClaimTaskReward(ctx context.Context, req *bayespb.ClaimTaskRewardRequest) (*bayespb.ClaimTaskRewardReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	_, err := s.data.RpcClient.UsercenterClient.ClaimTaskReward(ctx, &usercenterpb.ClaimTaskRewardRequest{
		Uid:     uid,
		TaskKey: req.TaskKey,
	})
	if err != nil {
		c.Log.Errorf("ClaimTaskReward rpc failed: %v", err)
		return nil, err
	}
	return &bayespb.ClaimTaskRewardReply{}, nil
}

func (s *BayesHttpService) ShareTaskDone(ctx context.Context, req *bayespb.ShareTaskDoneRequest) (*bayespb.ShareTaskDoneReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	_, err := s.data.RpcClient.UsercenterClient.TaskDone(ctx, &usercenterpb.TaskDoneRequest{
		Uid:     uid,
		TaskKey: "first-share",
	})
	if err != nil {
		c.Log.Errorf("TaskDone rpc failed: %v", err)
		return nil, err
	}
	return &bayespb.ShareTaskDoneReply{}, nil
}
