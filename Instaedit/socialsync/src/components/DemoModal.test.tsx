import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { DemoModal } from "./DemoModal";

describe("DemoModal", () => {
  it("is hidden when open is false", () => {
    render(<DemoModal open={false} onClose={vi.fn()} />);
    const backdrop = screen.getByTestId("demo-modal-backdrop");
    expect(backdrop.classList.contains("hidden")).toBe(true);
  });

  it("is visible when open is true", () => {
    render(<DemoModal open={true} onClose={vi.fn()} />);
    const backdrop = screen.getByTestId("demo-modal-backdrop");
    expect(backdrop.classList.contains("flex")).toBe(true);
  });

  it("renders the demo title", () => {
    render(<DemoModal open={true} onClose={vi.fn()} />);
    expect(screen.getByText(/SocialSync/)).toBeDefined();
  });

  it("calls onClose when the close button is clicked", async () => {
    const onClose = vi.fn();
    render(<DemoModal open={true} onClose={onClose} />);
    const user = userEvent.setup();
    await user.click(screen.getByTestId("demo-modal-close"));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("calls onClose when the Escape key is pressed", async () => {
    const onClose = vi.fn();
    render(<DemoModal open={true} onClose={onClose} />);
    const user = userEvent.setup();
    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("does not call onClose on Escape when modal is closed", async () => {
    const onClose = vi.fn();
    render(<DemoModal open={false} onClose={onClose} />);
    const user = userEvent.setup();
    await user.keyboard("{Escape}");
    expect(onClose).not.toHaveBeenCalled();
  });

  it("calls onClose when the backdrop is clicked", async () => {
    const onClose = vi.fn();
    render(<DemoModal open={true} onClose={onClose} />);
    const user = userEvent.setup();
    await user.click(screen.getByTestId("demo-modal-backdrop"));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
