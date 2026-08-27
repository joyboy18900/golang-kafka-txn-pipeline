package handler

import (
	"strconv"

	"golang-kafka-txn-pipeline/service"

	"github.com/gofiber/fiber/v2"
)

type transactionHandler struct {
	transactionSvc service.TransactionService
}

func NewTransactionHandler(transactionSvc service.TransactionService) transactionHandler {
	return transactionHandler{transactionSvc: transactionSvc}
}

func (h transactionHandler) Health(c *fiber.Ctx) error {
	return sendSuccess(c, fiber.StatusOK, "ok", nil)
}

func (h transactionHandler) List(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page"))
	limit, _ := strconv.Atoi(c.Query("limit"))

	resp, err := h.transactionSvc.List(c.Context(), service.ListTransactionsRequest{
		AccountID: c.Query("account_id"),
		Page:      page,
		Limit:     limit,
	})
	if err != nil {
		return handleError(c, err)
	}

	return sendSuccess(c, fiber.StatusOK, "transactions listed", resp)
}

func (h transactionHandler) GetBalance(c *fiber.Ctx) error {
	resp, err := h.transactionSvc.GetBalance(c.Context(), c.Params("account_id"))
	if err != nil {
		return handleError(c, err)
	}

	return sendSuccess(c, fiber.StatusOK, "balance retrieved", resp)
}

func (h transactionHandler) ListDeadLetterEvents(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page"))
	limit, _ := strconv.Atoi(c.Query("limit"))

	resp, err := h.transactionSvc.ListDeadLetterEvents(c.Context(), page, limit)
	if err != nil {
		return handleError(c, err)
	}

	return sendSuccess(c, fiber.StatusOK, "dead letter events listed", resp)
}
