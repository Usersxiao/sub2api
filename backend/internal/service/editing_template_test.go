package service

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/stretchr/testify/require"
)

type editingTemplateRepoFake struct {
	template *EditingTemplate
}

func (f *editingTemplateRepoFake) Create(_ context.Context, template *EditingTemplate) error {
	f.template = cloneEditingTemplateForTest(template)
	return nil
}

func (f *editingTemplateRepoFake) GetByID(_ context.Context, id string) (*EditingTemplate, error) {
	if f.template == nil || f.template.ID != id {
		return nil, ErrEditingTemplateNotFound
	}
	return cloneEditingTemplateForTest(f.template), nil
}

func (f *editingTemplateRepoFake) GetByOwner(ctx context.Context, id string, ownerID int64) (*EditingTemplate, error) {
	template, err := f.GetByID(ctx, id)
	if err != nil || template.OwnerID != ownerID {
		return nil, ErrEditingTemplateNotFound
	}
	return template, nil
}

func (f *editingTemplateRepoFake) List(context.Context, EditingTemplateListFilter, pagination.PaginationParams) ([]EditingTemplate, *pagination.PaginationResult, error) {
	return nil, &pagination.PaginationResult{}, nil
}

func (f *editingTemplateRepoFake) UpdateMetadata(_ context.Context, template *EditingTemplate, expected []string) error {
	if f.template == nil || !oneOf(f.template.Status, expected...) {
		return ErrEditingTemplateState
	}
	f.template = cloneEditingTemplateForTest(template)
	return nil
}

func (f *editingTemplateRepoFake) UpsertArtifact(_ context.Context, artifact *EditingTemplateArtifact, ownerID int64, expected []string) (*EditingTemplateArtifact, error) {
	if f.template == nil || f.template.OwnerID != ownerID || !oneOf(f.template.Status, expected...) {
		return nil, ErrEditingTemplateState
	}
	previous := f.template.Artifacts[artifact.Kind]
	f.template.Artifacts[artifact.Kind] = artifact
	f.template.Status = EditingTemplateStatusDraft
	return previous, nil
}

func (f *editingTemplateRepoFake) Transition(_ context.Context, id string, ownerID *int64, expected []string, toStatus, action, reason string, actorID *int64) error {
	if f.template == nil || f.template.ID != id || !oneOf(f.template.Status, expected...) || (ownerID != nil && f.template.OwnerID != *ownerID) {
		return ErrEditingTemplateState
	}
	from := f.template.Status
	f.template.Status = toStatus
	f.template.RejectionReason = reason
	f.template.Reviews = append([]EditingTemplateReview{{Action: action, FromStatus: from, ToStatus: toStatus, Reason: reason, ActorID: actorID}}, f.template.Reviews...)
	return nil
}

type editingTemplateStorageFake struct{}

func (editingTemplateStorageFake) Put(_ context.Context, templateID, kind, filename, contentType string, reader io.Reader, _ int64) (*EditingTemplateArtifact, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return &EditingTemplateArtifact{TemplateID: templateID, Kind: kind, StorageKey: templateID + "/" + kind, OriginalFilename: filename, ContentType: contentType, ByteSize: int64(len(data))}, nil
}
func (editingTemplateStorageFake) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}
func (editingTemplateStorageFake) Delete(context.Context, string) error { return nil }

func TestEditingTemplateReviewLifecycleAndOwnership(t *testing.T) {
	repo := &editingTemplateRepoFake{}
	service := NewEditingTemplateService(repo, editingTemplateStorageFake{}, nil, nil)
	ctx := context.Background()

	template, err := service.Create(ctx, 41, EditingTemplateMetadataInput{Title: "Product story", Description: "A concise launch video", EditingBrief: "Open with the outcome", AspectRatio: "16:9", DurationMS: 45000})
	require.NoError(t, err)
	require.Equal(t, EditingTemplateStatusDraft, template.Status)

	_, err = service.GetMine(ctx, 42, template.ID)
	require.ErrorIs(t, err, ErrEditingTemplateNotFound)

	for _, upload := range []struct{ kind, filename, contentType string }{
		{EditingTemplateArtifactProject, "story.ccproj", "application/octet-stream"},
		{EditingTemplateArtifactCover, "cover.png", "image/png"},
		{EditingTemplateArtifactPreview, "preview.mp4", "video/mp4"},
	} {
		_, err = service.UploadArtifact(ctx, 41, template.ID, upload.kind, upload.filename, upload.contentType, strings.NewReader("data"))
		require.NoError(t, err)
	}
	template, err = service.Submit(ctx, 41, template.ID)
	require.NoError(t, err)
	require.Equal(t, EditingTemplateStatusPendingReview, template.Status)

	_, err = service.Update(ctx, 41, template.ID, EditingTemplateMetadataInput{Title: "Changed"})
	require.ErrorIs(t, err, ErrEditingTemplateState)

	template, err = service.Reject(ctx, 7, template.ID, "Replace the placeholder frame")
	require.NoError(t, err)
	require.Equal(t, EditingTemplateStatusRejected, template.Status)
	require.Equal(t, "Replace the placeholder frame", template.RejectionReason)

	_, err = service.Update(ctx, 41, template.ID, EditingTemplateMetadataInput{Title: "Product story v2", Description: "Updated", EditingBrief: "Open strong", AspectRatio: "16:9", DurationMS: 46000})
	require.NoError(t, err)
	_, err = service.Submit(ctx, 41, template.ID)
	require.NoError(t, err)
	template, err = service.Approve(ctx, 7, template.ID)
	require.NoError(t, err)
	require.Equal(t, EditingTemplateStatusApproved, template.Status)
	artifact, reader, err := service.OpenPublicArtifact(ctx, template.ID, EditingTemplateArtifactProject)
	require.NoError(t, err)
	require.Equal(t, EditingTemplateArtifactProject, artifact.Kind)
	require.NoError(t, reader.Close())
	template, err = service.Unlist(ctx, 7, template.ID, "Outdated visual language")
	require.NoError(t, err)
	require.Equal(t, EditingTemplateStatusUnlisted, template.Status)
}

func TestEditingTemplateSubmitRequiresAllArtifacts(t *testing.T) {
	repo := &editingTemplateRepoFake{}
	service := NewEditingTemplateService(repo, editingTemplateStorageFake{}, nil, nil)
	template, err := service.Create(context.Background(), 1, EditingTemplateMetadataInput{Title: "Incomplete"})
	require.NoError(t, err)

	_, err = service.Submit(context.Background(), 1, template.ID)
	require.Error(t, err)
}

func cloneEditingTemplateForTest(source *EditingTemplate) *EditingTemplate {
	copy := *source
	copy.Artifacts = make(map[string]*EditingTemplateArtifact, len(source.Artifacts))
	for kind, artifact := range source.Artifacts {
		artifactCopy := *artifact
		copy.Artifacts[kind] = &artifactCopy
	}
	copy.Reviews = append([]EditingTemplateReview(nil), source.Reviews...)
	return &copy
}
