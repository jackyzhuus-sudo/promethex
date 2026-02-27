package http_api

import (
	"context"
	"market-backend/internal/pkg/util"

	apipb "market-proto/proto/market-backend/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"
)

func (s *HttpApiService) GetTasks(ctx context.Context, req *apipb.GetTasksRequest) (*apipb.GetTasksReply, error) {
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
		return &apipb.GetTasksReply{}, nil
	}

	rsp := &apipb.GetTasksReply{
		TotalRewardPoints: GetTasksRsp.TotalRewardPoints,
		Total:             GetTasksRsp.Total,
		Tasks:             []*apipb.GetTasksReply_Task{},
	}
	for _, task := range GetTasksRsp.Tasks {
		rsp.Tasks = append(rsp.Tasks, &apipb.GetTasksReply_Task{
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

func (s *HttpApiService) ClaimTaskReward(ctx context.Context, req *apipb.ClaimTaskRewardRequest) (*apipb.ClaimTaskRewardReply, error) {
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
	return &apipb.ClaimTaskRewardReply{}, nil
}

func (s *HttpApiService) ShareTaskDone(ctx context.Context, req *apipb.ShareTaskDoneRequest) (*apipb.ShareTaskDoneReply, error) {
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
	return &apipb.ShareTaskDoneReply{}, nil
}
